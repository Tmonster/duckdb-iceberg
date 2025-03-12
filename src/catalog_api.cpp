#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/irc_catalog.hpp"
#include "yyjson.hpp"
#include <curl/curl.h>
#include <sys/stat.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>

using namespace duckdb_yyjson;
namespace duckdb {

//! We use a global here to store the path that is selected on the ICAPI::InitializeCurl call
static string SELECTED_CURL_CERT_PATH = "";

static size_t RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// we statically compile in libcurl, which means the cert file location of the build machine is the
// place curl will look. But not every distro has this file in the same location, so we search a
// number of common locations and use the first one we find.
static string certFileLocations[] = {
        // Arch, Debian-based, Gentoo
        "/etc/ssl/certs/ca-certificates.crt",
        // RedHat 7 based
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        // Redhat 6 based
        "/etc/pki/tls/certs/ca-bundle.crt",
        // OpenSUSE
        "/etc/ssl/ca-bundle.pem",
        // Alpine
        "/etc/ssl/cert.pem"
};

const string IRCAPI::API_VERSION_1 = "v1";

struct YyjsonDocDeleter {
    void operator()(yyjson_doc* doc) {
        yyjson_doc_free(doc);
    }
    void operator()(yyjson_mut_doc* doc) {
        yyjson_mut_doc_free(doc);
    }
};

// Look through the the above locations and if one of the files exists, set that as the location curl should use.
static bool SelectCurlCertPath() {
	for (string& caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			SELECTED_CURL_CERT_PATH = caFile;
		}
	}
	return false;
}

static bool SetCurlCAFileInfo(CURL* curl) {
	if (!SELECTED_CURL_CERT_PATH.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, SELECTED_CURL_CERT_PATH.c_str());
        return true;
	}
    return false;
}

// Note: every curl object we use should set this, because without it some linux distro's may not find the CA certificate.
static void InitializeCurlObject(CURL * curl, const string &token) {
  	if (!token.empty()) {
		curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
	}
    SetCurlCAFileInfo(curl);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(yyjson_val *obj, const string &field, TYPE default_val,
                                  bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && yyjson_get_type(val) == TYPE_NUM) {
		return get_function(val);
	} else if (!fail_on_missing) {
		return default_val;
	}
	throw IOException("Invalid field found while parsing field: " + field);
}

static uint64_t TryGetNumFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                    uint64_t default_val = 0) {
	return TemplatedTryGetYYJson<uint64_t, YYJSON_TYPE_NUM, yyjson_get_uint>(obj, field, default_val,
	                                                                                        fail_on_missing);
}
static bool TryGetBoolFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = false,
								 bool default_val = false) {
	return TemplatedTryGetYYJson<bool, YYJSON_TYPE_BOOL, yyjson_get_bool>(obj, field, default_val,
																						 fail_on_missing);
}
static string TryGetStrFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                  const char *default_val = "") {
	return TemplatedTryGetYYJson<const char *, YYJSON_TYPE_STR, yyjson_get_str>(obj, field, default_val,
	                                                                                           fail_on_missing);
}

static string DeleteRequest(const string &url, const string &token = "", curl_slist *extra_headers = NULL) {
    CURL *curl;
    CURLcode res;
    string readBuffer;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        
        if(extra_headers) {
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
        }
        
        InitializeCurlObject(curl, token);
        res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLcode::CURLE_OK) {
            string error = curl_easy_strerror(res);
            throw IOException("Curl DELETE Request to '%s' failed with error: '%s'", url, error);
        }
        
        return readBuffer;
    }
    throw InternalException("Failed to initialize curl");
}

class DuckDBSecretCredentialProvider : public Aws::Auth::AWSCredentialsProviderChain
{
public:
	DuckDBSecretCredentialProvider(const string& key_id, const string &secret, const string &sesh_token) {
		credentials.SetAWSAccessKeyId(key_id);
		credentials.SetAWSSecretKey(secret);
		credentials.SetSessionToken(sesh_token);
	}

	~DuckDBSecretCredentialProvider() = default;

	Aws::Auth::AWSCredentials GetAWSCredentials() override {
		return credentials;
	};

protected:
	Aws::Auth::AWSCredentials credentials;
};

static string GetAwsService(const string host) {
	return host.substr(0, host.find_first_of('.'));
}

static string GetAwsRegion(const string host) {
	idx_t first_dot = host.find_first_of('.');
	idx_t second_dot = host.find_first_of('.', first_dot + 1);
	return host.substr(first_dot + 1, second_dot - first_dot - 1);
}

static string GetRequestAws(ClientContext &context, IRCEndpointBuilder endpoint_builder, const string &secret_name) {
	auto clientConfig = make_uniq<Aws::Client::ClientConfiguration>();

	if (!SELECTED_CURL_CERT_PATH.empty()) {
		clientConfig->caFile = SELECTED_CURL_CERT_PATH;
	}

	std::shared_ptr<Aws::Http::HttpClientFactory> MyClientFactory;
	std::shared_ptr<Aws::Http::HttpClient> MyHttpClient;

	MyHttpClient = Aws::Http::CreateHttpClient(*clientConfig);
	Aws::Http::URI uri;

	// TODO move this to IRCatalog::GetBaseURL()
	auto service = GetAwsService(endpoint_builder.GetHost());
	auto region = GetAwsRegion(endpoint_builder.GetHost());

	// Add iceberg. This is necessary here and cannot be included in the host
	uri.AddPathSegment("iceberg");
	// push bach the version
	uri.AddPathSegment(endpoint_builder.GetVersion());
	// then the warehouse
	if (service == "glue") {
		uri.AddPathSegment("catalogs");
		uri.AddPathSegment(endpoint_builder.GetWarehouse());
	} else {
		uri.AddPathSegment(endpoint_builder.GetWarehouse());
	}

	for (auto &component : endpoint_builder.path_components) {
		uri.AddPathSegment(component);
	}

	Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTPS;
	uri.SetScheme(scheme);
	// set host to glue
	uri.SetAuthority(endpoint_builder.GetHost());
	auto encoded = uri.GetURLEncodedPath();

	const Aws::Http::URI uri_const = Aws::Http::URI(uri);
	auto create_http_req = Aws::Http::CreateHttpRequest(uri_const,
									 Aws::Http::HttpMethod::HTTP_GET,
									 Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

	std::shared_ptr<Aws::Http::HttpRequest> req(create_http_req);

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetSecret(context, secret_name);

	std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	provider = std::make_shared<DuckDBSecretCredentialProvider>(
		kv_secret.secret_map["key_id"].GetValue<string>(),
		kv_secret.secret_map["secret"].GetValue<string>(),
		kv_secret.secret_map["session_token"].GetValue<string>()
	);


	auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());
	signer->SignRequest(*req);
	std::shared_ptr<Aws::Http::HttpResponse> res = MyHttpClient->MakeRequest(req);
	Aws::Http::HttpResponseCode resCode = res->GetResponseCode();
	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Aws.HTTPRequest", "GET %s (response %d) (signed with key_id '%s' for service '%s', in region '%s')", uri.GetURIString(), resCode, kv_secret.secret_map["key_id"].GetValue<string>(), service.c_str(), region.c_str());
	if (resCode == Aws::Http::HttpResponseCode::OK) {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		return resBody.str();
	} else {
		Aws::StringStream resBody;
		resBody <<  res->GetResponseBody().rdbuf();
		throw IOException("Failed to query %s, http error %d thrown. Message: %s", req->GetUri().GetURIString(true), res->GetResponseCode(), resBody.str());
	}
}

static string GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder, const string &secret_name, const string &token = "", curl_slist *extra_headers = NULL) {
	if (StringUtil::StartsWith(endpoint_builder.GetHost(), "glue." )) {
		auto str = GetRequestAws(context, endpoint_builder, secret_name);
		return str;
	}
	auto url = endpoint_builder.GetURL();
	CURL *curl;
	CURLcode res;
	string readBuffer;

	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
		
		if(extra_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
		}
		
		InitializeCurlObject(curl, token);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);

		DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "GET %s (curl code '%s')", url, curl_easy_strerror(res));
		if (res != CURLcode::CURLE_OK) {
			string error = curl_easy_strerror(res);
			throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
		}

		return readBuffer;
	}
	throw InternalException("Failed to initialize curl");
}

static string PostRequest(
		ClientContext &context,
		const string &url, 
		const string &post_data, 
		const string &content_type = "x-www-form-urlencoded",
		const string &token = "", 
		curl_slist *extra_headers = NULL) {
    string readBuffer;
    CURL *curl = curl_easy_init();
    if (!curl) {
		throw InternalException("Failed to initialize curl");
	}

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

	// Create default headers for content type
	struct curl_slist *headers = NULL;
	const string content_type_str = "Content-Type: application/" + content_type;
	headers = curl_slist_append(headers, content_type_str.c_str());
	
	// Append any extra headers
	if (extra_headers) {
		struct curl_slist *temp = extra_headers;
		while (temp) {
			headers = curl_slist_append(headers, temp->data);
			temp = temp->next;
		}
	}
	
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	InitializeCurlObject(curl, token);
	
	// Perform the request
	CURLcode res = curl_easy_perform(curl);
	
	// Clean up
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Curl.HTTPRequest", "POST %s (curl code '%s')", url, curl_easy_strerror(res));
	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
	}
	return readBuffer;
}

static yyjson_doc *api_result_to_doc(const string &api_result) {
	auto *doc = yyjson_read(api_result.c_str(), api_result.size(), 0);
	auto *root = yyjson_doc_get_root(doc);
	auto *error = yyjson_obj_get(root, "error");
	if (error != NULL) {
		string err_msg = TryGetStrFromObject(error, "message");
		throw std::runtime_error(err_msg);
	}
	return doc;
}

static string GetTableMetadata(ClientContext &context, const IRCatalog &catalog, const string &schema, const string &table, const string &secret_name) {
	struct curl_slist *extra_headers = NULL;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	url.AddPathComponent(table);
	extra_headers = curl_slist_append(extra_headers, "X-Iceberg-Access-Delegation: vended-credentials");
	string api_result = GetRequest(
		context,
		url,
		secret_name,
		catalog.credentials.token,
		extra_headers);
	curl_slist_free_all(extra_headers);
	return api_result;
}

void IRCAPI::InitializeCurl() {
	SelectCurlCertPath();
}

vector<string> IRCAPI::GetCatalogs(ClientContext &context, const IRCatalog &catalog, IRCCredentials credentials) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

static IRCAPIColumnDefinition ParseColumnDefinition(yyjson_val *column_def) {
	IRCAPIColumnDefinition result;
	result.name = TryGetStrFromObject(column_def, "name");
	result.type_text = TryGetStrFromObject(column_def, "type");
	result.precision = (result.type_text == "decimal") ? TryGetNumFromObject(column_def, "type_precision") : -1;
	result.scale = (result.type_text == "decimal") ? TryGetNumFromObject(column_def, "type_scale") : -1;
	result.position = TryGetNumFromObject(column_def, "id") - 1;
	return result;
}

IRCAPITableCredentials IRCAPI::GetTableCredentials(ClientContext &context, const IRCatalog &catalog, const string &schema, const string &table, IRCCredentials credentials) {
	IRCAPITableCredentials result;
	string api_result = GetTableMetadata(context, catalog, schema, table, catalog.secret_name);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *warehouse_credentials = yyjson_obj_get(root, "config");
	auto credential_size = yyjson_obj_size(warehouse_credentials);
	auto catalog_credentials = IRCatalog::GetSecret(context, catalog.secret_name);
	if (warehouse_credentials && credential_size > 0) {
		result.key_id = TryGetStrFromObject(warehouse_credentials, "s3.access-key-id", false);
		result.secret = TryGetStrFromObject(warehouse_credentials, "s3.secret-access-key",  false);
		result.session_token = TryGetStrFromObject(warehouse_credentials, "s3.session-token", false);
		if (catalog_credentials) {
       		auto kv_secret = dynamic_cast<const KeyValueSecret &>(*catalog_credentials->secret);
			auto region = kv_secret.TryGetValue("region").ToString();
			result.region = region;
		}
	}
	return result;
}

string IRCAPI::GetToken(ClientContext &context, string id, string secret, string endpoint) {
	string post_data = "grant_type=client_credentials&client_id=" + id + "&client_secret=" + secret + "&scope=PRINCIPAL_ROLE:ALL";
	string api_result = PostRequest(context, endpoint + "/v1/oauth/tokens", post_data);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	return TryGetStrFromObject(root, "access_token");
}

static void populateTableMetadata(IRCAPITable &table, yyjson_val *metadata_root) {
	table.storage_location = TryGetStrFromObject(metadata_root, "metadata-location");
	auto *metadata = yyjson_obj_get(metadata_root, "metadata");
	//table_result.table_id = TryGetStrFromObject(metadata, "table-uuid");

	uint64_t current_schema_id = TryGetNumFromObject(metadata, "current-schema-id");
	auto *schemas = yyjson_obj_get(metadata, "schemas");
	yyjson_val *schema;
	size_t schema_idx, schema_max;
	bool found = false;
	yyjson_arr_foreach(schemas, schema_idx, schema_max, schema) {
		uint64_t schema_id = TryGetNumFromObject(schema, "schema-id");
		if (schema_id == current_schema_id) {
			found = true;
			auto *columns = yyjson_obj_get(schema, "fields");
			yyjson_val *col;
			size_t col_idx, col_max;
			yyjson_arr_foreach(columns, col_idx, col_max, col) {
				auto column_definition = ParseColumnDefinition(col);
				table.columns.push_back(column_definition);
			}
		}
	}

	if (!found) {
		throw InternalException("Current schema not found");
	}
}

static IRCAPITable createTable(const IRCatalog &catalog, const string &schema, const string &table_name) {
	IRCAPITable table_result;
	table_result.catalog_name = catalog.GetName();
	table_result.schema_name = schema;
	table_result.name = table_name;
	table_result.data_source_format = "ICEBERG";
	table_result.table_id = "uuid-" + schema + "-" + "table";
	std::replace(table_result.table_id.begin(), table_result.table_id.end(), '_', '-');
	return table_result;
}

IRCAPITable IRCAPI::GetTable(ClientContext &context,
	const IRCatalog &catalog, const string &schema, const string &table_name, optional_ptr<IRCCredentials> credentials) {
	
	IRCAPITable table_result = createTable(catalog, schema, table_name);
	if (credentials) {
		string result = GetTableMetadata(context, catalog, schema, table_result.name, catalog.secret_name);
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(result));
		auto *metadata_root = yyjson_doc_get_root(doc.get());
		populateTableMetadata(table_result, metadata_root);
	} else {
		// Skip fetching metadata, we'll do it later when we access the table
		IRCAPIColumnDefinition col;
		col.name = "__";
		col.type_text = "int";
		col.precision = -1;
		col.scale = -1;
		col.position = 0;
		table_result.columns.push_back(col);
	}

	return table_result;
}

// TODO: handle out-of-order columns using position property
vector<IRCAPITable> IRCAPI::GetTables(ClientContext &context, const IRCatalog &catalog, const string &schema) {
	vector<IRCAPITable> result;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	string api_result = GetRequest(context, url, catalog.secret_name, catalog.credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *tables = yyjson_obj_get(root, "identifiers");
	size_t idx, max;
	yyjson_val *table;
	yyjson_arr_foreach(tables, idx, max, table) {
		auto table_result = GetTable(context, catalog, schema, TryGetStrFromObject(table, "name"), nullptr);
		result.push_back(table_result);
	}

	return result;
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, const IRCatalog &catalog, IRCCredentials credentials) {
	vector<IRCAPISchema> result;
	auto endpoint_builder = catalog.GetBaseUrl();
	endpoint_builder.AddPathComponent("namespaces");
	string api_result =
	    GetRequest(context, endpoint_builder, catalog.secret_name, catalog.credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *schemas = yyjson_obj_get(root, "namespaces");
	size_t idx, max;
	yyjson_val *schema;
	yyjson_arr_foreach(schemas, idx, max, schema) {
		IRCAPISchema schema_result;
		schema_result.catalog_name = catalog.GetName();
		yyjson_val *value = yyjson_arr_get(schema, 0);
		schema_result.schema_name = yyjson_get_str(value);
		result.push_back(schema_result);
	}

	return result;
}

IRCAPISchema IRCAPI::CreateSchema(ClientContext &context, const IRCatalog &catalog, const string &internal, const string &schema, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI::Create Schema not Implemented");
}

void IRCAPI::DropSchema(ClientContext &context, const string &internal, const string &schema, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI Drop Schema not Implemented");
}

void IRCAPI::DropTable(ClientContext &context, const IRCatalog &catalog, const string &internal, const string &schema, string &table_name, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI Drop Table not Implemented");
}

static std::string json_to_string(yyjson_mut_doc *doc, yyjson_write_flag flags = YYJSON_WRITE_PRETTY) {
    char *json_chars = yyjson_mut_write(doc, flags, NULL);
    std::string json_str(json_chars);
    free(json_chars);
    return json_str;
}

IRCAPITable IRCAPI::CreateTable(ClientContext &context, const IRCatalog &catalog, const string &internal, const string &schema, IRCCredentials credentials, CreateTableInfo *table_info) {
	throw NotImplementedException("IRCAPI Create Table not Implemented");
}

} // namespace duckdb
