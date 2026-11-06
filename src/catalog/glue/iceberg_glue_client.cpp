#include "catalog/glue/iceberg_glue_client.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"

#include "catalog/iceberg_catalog.hpp"
#include "catalog/rest/storage/aws.hpp"

namespace duckdb {

IcebergGlueException::IcebergGlueException(const string &error_type_p, const string &message)
    : Exception(ExceptionType::IO, StringUtil::Format("AWS Glue %s: %s", error_type_p, message)),
      error_type(error_type_p) {
}

IcebergGlueClient::IcebergGlueClient(IcebergGlueClientOptions options_p) : options(std::move(options_p)) {
}

namespace {

string SecretValue(const KeyValueSecret &secret, const string &key) {
	auto entry = secret.secret_map.find(Identifier(key));
	if (entry == secret.secret_map.end() || entry->second.IsNull()) {
		return string();
	}
	return entry->second.ToString();
}

//! 'com.amazonaws.glue#EntityNotFoundException' or 'EntityNotFoundException:<url>' -> 'EntityNotFoundException'
string GlueErrorType(string type) {
	auto hash = type.find('#');
	if (hash != string::npos) {
		type = type.substr(hash + 1);
	}
	auto colon = type.find(':');
	if (colon != string::npos) {
		type = type.substr(0, colon);
	}
	return type;
}

} // namespace

unique_ptr<JSONDocument> IcebergGlueClient::Call(ClientContext &context, const string &operation, JSONWriter &request,
                                                 JSONMutableValue request_body) {
	if (!options.catalog_id.empty()) {
		request_body.AddString("CatalogId", options.catalog_id);
	}
	request.SetRoot(request_body);
	auto body = request.ToString();

	auto secret_entry = IcebergCatalog::GetStorageSecret(context, options.secret_name);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	AWSInput input;
	input.service = "glue";
	input.region = options.region.empty() ? SecretValue(kv_secret, "region") : options.region;
	if (input.region.empty()) {
		throw InvalidConfigurationException("No AWS region for the Glue catalog: set the 'region' ATTACH option, or "
		                                    "the REGION of the secret");
	}
	if (options.endpoint.empty()) {
		input.authority = StringUtil::Format("glue.%s.amazonaws.com", input.region);
	} else {
		auto endpoint = options.endpoint;
		if (StringUtil::StartsWith(StringUtil::Lower(endpoint), "http://")) {
			input.use_https = false;
			endpoint = endpoint.substr(7);
		} else if (StringUtil::StartsWith(StringUtil::Lower(endpoint), "https://")) {
			endpoint = endpoint.substr(8);
		}
		StringUtil::RTrim(endpoint, "/");
		input.authority = endpoint;
	}
	// The Glue API is served at '/', an empty segment makes both the wire and the canonical path '/'
	input.path_segments.push_back("");
	input.key_id = SecretValue(kv_secret, "key_id");
	input.secret = SecretValue(kv_secret, "secret");
	input.session_token = SecretValue(kv_secret, "session_token");

	HTTPHeaders headers(*context.db);
	headers["Content-Type"] = "application/x-amz-json-1.1";
	headers["X-Amz-Target"] = "AWSGlue." + operation;
	auto response = input.Request(RequestType::POST_REQUEST, context, headers, body);
	if (!response) {
		throw IOException("AWS Glue %s request failed without a response", operation);
	}
	if (response->HasRequestError()) {
		throw IOException("AWS Glue %s request failed: %s", operation, response->GetRequestError());
	}
	auto status = static_cast<int32_t>(response->status);
	auto &response_body = response->body;
	unique_ptr<JSONDocument> document;
	if (!response_body.empty()) {
		document = JSONDocument::Parse(response_body.c_str(), response_body.size());
	}
	if (status < 200 || status >= 300) {
		string error_type = response->HasHeader("x-amzn-ErrorType") ? response->GetHeaderValue("x-amzn-ErrorType") : "";
		string message = response_body;
		if (document && document->GetRoot().IsObject()) {
			auto root = document->GetRoot();
			auto type_value = root.GetMember("__type");
			if (type_value.IsValid() && type_value.IsString()) {
				error_type = type_value.GetString();
			}
			for (auto key : {"message", "Message"}) {
				auto message_value = root.GetMember(key);
				if (message_value.IsValid() && message_value.IsString()) {
					message = message_value.GetString();
				}
			}
		}
		if (error_type.empty()) {
			error_type = StringUtil::Format("HTTP %d", status);
		}
		throw IcebergGlueException(GlueErrorType(error_type), message);
	}
	if (!document) {
		document = JSONDocument::Parse("{}", 2);
	}
	return document;
}

} // namespace duckdb
