#include "catalog/rest/iceberg_rest_catalog.hpp"

#include "duckdb/logging/logger.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/transaction/iceberg_rest_commit.hpp"
#include "iceberg_logging.hpp"
#include "rest_catalog/objects/catalog_config.hpp"

namespace duckdb {

IcebergRestCatalog::IcebergRestCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                                       unique_ptr<IcebergAuthorization> auth_handler,
                                       IcebergAttachOptions &attach_options_p,
                                       const optional<Identifier> &default_schema)
    : IcebergCatalog(IcebergCatalogType::REST, db_p, access_mode, attach_options_p, default_schema),
      auth_handler(std::move(auth_handler)),
      base_uri(attach_options_p.catalog_uri), version("v1") {
}

IcebergRestCatalog::~IcebergRestCatalog() = default;

//===--------------------------------------------------------------------===//
// Catalog type hooks
//===--------------------------------------------------------------------===//

bool IcebergRestCatalog::SchemaExists(ClientContext &context, const string &schema_name) {
	return IRCAPI::VerifySchemaExistence(context, *this, schema_name);
}

IcebergListSchemasResult IcebergRestCatalog::ListSchemas(ClientContext &context, const vector<string> &parent) {
	return IRCAPI::ListSchemas(context, *this, parent);
}

APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
IcebergRestCatalog::LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) {
	return IRCAPI::GetNamespace(context, *this, schema);
}

IcebergListTablesResult IcebergRestCatalog::ListTables(ClientContext &context, const vector<string> &namespace_items,
                                                       bool allow_forbidden) {
	return IRCAPI::ListTables(context, *this, namespace_items, allow_forbidden);
}

IcebergLoadTableResult IcebergRestCatalog::LoadTable(ClientContext &context, const vector<string> &namespace_items,
                                                     const string &table_name) {
	return IRCAPI::LoadTable(context, *this, namespace_items, table_name);
}

bool IcebergRestCatalog::SupportsViews(IcebergViewOperation operation) const {
	switch (operation) {
	case IcebergViewOperation::LIST:
		return supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views");
	case IcebergViewOperation::LOAD:
		return supported_urls.count("GET /v1/{prefix}/namespaces/{namespace}/views/{view}");
	case IcebergViewOperation::CREATE:
		return supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/views");
	default:
		throw InternalException("Unrecognized IcebergViewOperation");
	}
}

IcebergListViewsResult IcebergRestCatalog::ListViews(ClientContext &context, const vector<string> &namespace_items) {
	return IRCAPI::ListViews(context, *this, namespace_items);
}

IcebergLoadViewResult IcebergRestCatalog::LoadView(ClientContext &context, const vector<string> &namespace_items,
                                                   const string &view_name) {
	return IRCAPI::LoadView(context, *this, namespace_items, view_name);
}

IcebergTable &IcebergRestCatalog::CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
                                              const IcebergCreateTableRequest &request) {
	// Immediately create the table with stage_create = true to get metadata & data location(s)
	// transaction commit will either commit with data (OR) create the table with stage_create = false
	auto new_table_result = make_uniq<const rest_api_objects::LoadTableResult>(
	    IRCAPI::CommitNewTable(context, *this, schema.namespace_items, request));
	auto &table_info =
	    AddCreatedTable(context, IcebergTable(*this, schema, request.GetName(), *new_table_result));
	table_request_cache.SetOrOverwrite(table_info.GetTableKey(), std::move(new_table_result));

	// if we stage created the table, we add an assert create
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	auto &transaction_data = table_info.GetOrCreateTransactionData(iceberg_transaction);
	if (attach_options.stage_create_tables) {
		transaction_data.TableAddAssertCreate();
	}
	if (!attach_options.stage_create_tables && attach_options.skip_create_table_metadata_updates) {
		return table_info;
	}
	AddCreateTableUpdates(context, table_info);
	return table_info;
}

bool IcebergRestCatalog::CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const {
	if (!MultiTableCommitAvailable()) {
		return false;
	}
	for (const auto &entry : alter_update.updated_tables) {
		const auto &table_info = entry.second.get();
		if (!table_info.transaction_data) {
			continue;
		}
		if (table_info.transaction_data->has_assert_create) {
			return false;
		}
	}
	return true;
}

void IcebergRestCatalog::VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const {
	if (alter_update.updated_tables.size() <= 1) {
		return;
	}
	if (CanUseMultiTableCommit(alter_update)) {
		return;
	}
	throw TransactionException("Iceberg REST Catalog cannot commit this transaction atomically because it would "
	                           "require multiple table commit requests without atomic multi-table commit support");
}

bool IcebergRestCatalog::MultiTableCommitAvailable() const {
	return !attach_options.disable_multi_table_commit && supported_urls.count("POST /v1/{prefix}/transactions/commit");
}

void IcebergRestCatalog::Commit(IcebergTransaction &transaction) {
	IcebergRestTransactionCommit(*this, transaction).Commit();
}

//===--------------------------------------------------------------------===//
// Iceberg REST Catalog
//===--------------------------------------------------------------------===//

IRCEndpointBuilder IcebergRestCatalog::GetBaseUrl() const {
	auto url_builder = IRCEndpointBuilder();
	url_builder.SetHost(base_uri);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(version));

	return url_builder;
}

void IcebergRestCatalog::AddDefaultSupportedEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// set or remove properties on a namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/properties");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a tbale
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// Register a table using given metadata file location.
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/register");
	// send metrics report to this endpoint to be processed by the backend
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics");
	// Rename a table from one identifier to another.
	supported_urls.insert("POST /v1/{prefix}/tables/rename");
	// commit updates to multiple tables in an atomic transaction
	supported_urls.insert("POST /v1/{prefix}/transactions/commit");
	// Views aren't in the spec defaults; catalogs that support them advertise via /v1/config
}

void IcebergRestCatalog::AddS3TablesEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// Rename a table from one identifier to another.
	supported_urls.insert("POST /v1/{prefix}/tables/rename");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// namespace exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}");
}

void IcebergRestCatalog::AddGlueEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
}

void IcebergRestCatalog::ParsePrefix() {
	prefix.clear();
	auto &prefix_property = attach_options.prefix;
	if (prefix_property.empty()) {
		return;
	}

	auto decoded_prefix = StringUtil::URLDecode(prefix_property);
	if (attach_options.encode_entire_prefix || !StringUtil::Equals(decoded_prefix, prefix_property)) {
		prefix.push_back(std::move(decoded_prefix));
	} else {
		prefix = StringUtil::Split(decoded_prefix, '/');
	}
}

void IcebergRestCatalog::Configure(ClientContext &context, const rest_api_objects::CatalogConfig &catalog_config,
                               IcebergEndpointType endpoint_type) {
	base_uri = attach_options.catalog_uri;
	StringUtil::RTrim(base_uri, "/");
	if (base_uri.empty()) {
		throw InvalidConfigurationException("Missing 'uri' option for Iceberg attach");
	}
	attach_options.catalog_uri = base_uri;
	warehouse = attach_options.warehouse;
	ParsePrefix();
	namespace_separator = attach_options.namespace_separator;

	catalog_properties = catalog_config.defaults;
	for (auto &entry : catalog_config.overrides) {
		catalog_properties[entry.first] = entry.second;
	}

	supported_urls.clear();
	if (auto &endpoints = catalog_config.endpoints) {
		for (auto &endpoint : *endpoints) {
			supported_urls.insert(endpoint);
		}
	}
	// should be if s3tables
	if (!catalog_config.endpoints && endpoint_type == IcebergEndpointType::AWS_S3TABLES) {
		supported_urls.clear();
		AddS3TablesEndpoints();
	} else if (!catalog_config.endpoints && endpoint_type == IcebergEndpointType::AWS_GLUE) {
		supported_urls.clear();
		AddGlueEndpoints();
	} else if (!catalog_config.endpoints) {
		AddDefaultSupportedEndpoints();
	}

	if (prefix.empty()) {
		DUCKDB_LOG(context, IcebergLogType, "No prefix found for catalog with warehouse value %s", warehouse);
	}
}

} // namespace duckdb
