#include "catalog/rest/storage/iceberg_table_secret_provider.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

//! Build a KeyValueSecret from a fully-populated CreateSecretInput (vended credentials plus a
//! 'refresh_info' struct). Mirrors the relevant parts of httpfs'
//! CreateS3SecretFunctions::CreateSecretFunctionInternal so the produced secret is indistinguishable
//! from one created via the "config" provider, except that its provider enables refresh.
static unique_ptr<BaseSecret> BuildVendedSecret(CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	secret->redact_keys = {"secret", "session_token", "bearer_token"};

	//! For r2 we can derive the endpoint from the account id (matching httpfs).
	auto account_id_entry = input.options.find("account_id");
	if (input.type == "r2" && account_id_entry != input.options.end()) {
		secret->secret_map["endpoint"] = account_id_entry->second.ToString() + ".r2.cloudflarestorage.com";
		secret->secret_map["url_style"] = "path";
	}

	for (auto &option : input.options) {
		auto key = StringUtil::Lower(option.first);
		if (key == "account_id") {
			continue; //! handled above
		}
		//! The table-identity fields are only consumed while re-vending; they are never stored on the
		//! secret itself. They are reconstructed from 'refresh_info' on the next refresh.
		if (key == "catalog_name" || key == "schema" || key == "table") {
			continue;
		}
		secret->secret_map[key] = option.second;
	}
	return std::move(secret);
}

//! Re-hit the GetTableInformation endpoint for the table identified by the refresh fields and re-vend
//! fresh credentials. Returns a CreateSecretInput carrying the new credentials, keyed to the identity
//! (name + scope) of the secret being refreshed.
static CreateSecretInput RevendVendedCredentials(ClientContext &context, CreateSecretInput &input) {
	auto catalog_name = input.options.at("catalog_name").ToString();
	auto schema_name = input.options.at("schema").ToString();
	auto table_name = input.options.at("table").ToString();

	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();

	auto schema_entry = ic_catalog.GetSchemas().GetEntry(context, schema_name, OnEntryNotFound::THROW_EXCEPTION);
	auto &iceberg_schema = schema_entry->Cast<IcebergSchemaEntry>();

	auto &table_entries = iceberg_schema.tables.GetEntries();
	auto table_entry = table_entries.find(table_name);
	if (table_entry == table_entries.end()) {
		throw InvalidConfigurationException(
		    "Could not refresh Iceberg vended credentials: table '%s' not found in schema '%s' of catalog '%s'",
		    table_name, schema_name, catalog_name);
	}
	// TODO: we should actually update just the config object in the table info in table_entry->second
	// Just need a different function for it
	auto table_info = IcebergTableInformation(ic_catalog, iceberg_schema, table_name);
	// auto table_info = table_entry->second;

	//! Force a fresh request to the catalog (bypassing the metadata cache) so we obtain newly vended
	//! credentials, then refresh the table's config/storage-credentials in place.
	auto get_table_result = IRCAPI::GetTable(context, ic_catalog, iceberg_schema, table_name);
	if (get_table_result.has_error) {
		throw HTTPException(StringUtil::Format(
		    "Could not refresh Iceberg vended credentials for table '%s': GetTableInformation returned %s with "
		    "message \"%s\"",
		    table_name, EnumUtil::ToString(get_table_result.status_), get_table_result.error_._error.message));
	}

	table_info.InitializeFromLoadTableResult(*get_table_result.result_);

	auto credentials = table_info.GetVendedCredentials(context);

	//! Select the re-vended credential matching the secret being refreshed. Match on scope; fall back
	//! to the single credential when there is exactly one.
	optional_ptr<CreateSecretInput> match;
	if (credentials.config) {
		match = credentials.config.get();
	} else {
		for (auto &candidate : credentials.storage_credentials) {
			if (candidate.scope == input.scope) {
				match = &candidate;
				break;
			}
		}
		if (!match && credentials.storage_credentials.size() == 1) {
			match = &credentials.storage_credentials[0];
		}
	}
	if (!match) {
		throw InvalidConfigurationException(
		    "Could not refresh Iceberg vended credentials for table '%s': no matching credential was re-vended",
		    table_name);
	}

	//! Preserve the identity (name + scope) of the secret being refreshed; the freshly vended input
	//! carries the new credentials and a new 'refresh_info' struct.
	auto result = std::move(*match);
	result.name = input.name;
	result.scope = input.scope;
	result.type = input.type;
	result.provider = input.provider;
	result.storage_type = input.storage_type;
	result.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	result.persist_type = SecretPersistType::TEMPORARY;

	//! Re-apply HTTP-secret options (proxy / verify_ssl) that PrepareIcebergScanFromEntry would add at
	//! initial creation time, so the refreshed secret is functionally identical.
	auto http_secret_entry = IcebergCatalog::GetHTTPSecret(context, "");
	if (http_secret_entry) {
		AddHTTPSecretsToOptions(*http_secret_entry, result.options);
	}

	return result;
}

unique_ptr<BaseSecret> IcebergTableSecretProvider::CreateSecret(ClientContext &context, CreateSecretInput &input) {
	//! On refresh, httpfs' GenerateRefreshSecretInfo unpacks the stored 'refresh_info' struct into loose
	//! options, so the presence of 'catalog_name' signals a refresh: re-vend fresh credentials first.
	if (input.options.find("catalog_name") != input.options.end()) {
		DUCKDB_LOG_INFO(context, "Refreshing Iceberg vended credentials for secret '%s'", input.name);
		auto revended = RevendVendedCredentials(context, input);
		//! Mark the secret as having been refreshed so it can be distinguished from one created at the
		//! initial scan. Persists across subsequent refreshes (it is re-applied each time).
		revended.options["refreshed_secret"] = Value("true");
		return BuildVendedSecret(revended);
	}
	//! Initial creation: the input already carries freshly vended credentials + a 'refresh_info' struct.
	return BuildVendedSecret(input);
}

Value IcebergTableSecretProvider::MakeRefreshInfo(const string &catalog_name, const string &schema_name,
                                                  const string &table_name) {
	child_list_t<Value> fields;
	fields.emplace_back("catalog_name", Value(catalog_name));
	fields.emplace_back("schema", Value(schema_name));
	fields.emplace_back("table", Value(table_name));
	return Value::STRUCT(std::move(fields));
}

void IcebergTableSecretProvider::Register(ExtensionLoader &loader) {
	//! Register the provider under each storage type whose credentials can be vended and refreshed via
	//! the s3fs refresh path. (Azure uses a different filesystem and is not covered here.)
	for (const char *type : {"s3", "gcs", "r2"}) {
		CreateSecretFunction function = {type, PROVIDER, CreateSecret};

		//! Table-identity fields (present in 'refresh_info', and unpacked into options on refresh).
		function.named_parameters["refresh_info"] = LogicalType::ANY;
		function.named_parameters["catalog_name"] = LogicalType::VARCHAR;
		function.named_parameters["schema"] = LogicalType::VARCHAR;
		function.named_parameters["table"] = LogicalType::VARCHAR;

		//! Storage-credential fields produced by GetVendedCredentials / ParseConfigOptions.
		function.named_parameters["key_id"] = LogicalType::VARCHAR;
		function.named_parameters["secret"] = LogicalType::VARCHAR;
		function.named_parameters["session_token"] = LogicalType::VARCHAR;
		function.named_parameters["region"] = LogicalType::VARCHAR;
		function.named_parameters["endpoint"] = LogicalType::VARCHAR;
		function.named_parameters["url_style"] = LogicalType::VARCHAR;
		function.named_parameters["use_ssl"] = LogicalType::BOOLEAN;
		function.named_parameters["verify_ssl"] = LogicalType::BOOLEAN;
		function.named_parameters["bearer_token"] = LogicalType::VARCHAR;
		function.named_parameters["account_id"] = LogicalType::VARCHAR;
		function.named_parameters["expires_at"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy_username"] = LogicalType::VARCHAR;
		function.named_parameters["http_proxy_password"] = LogicalType::VARCHAR;

		//! Marker set on a secret that was produced by a refresh (not the initial scan).
		function.named_parameters["refreshed_secret"] = LogicalType::VARCHAR;

		loader.RegisterFunction(function);
	}
}

} // namespace duckdb
