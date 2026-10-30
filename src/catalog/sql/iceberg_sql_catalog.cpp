#include "catalog/sql/iceberg_sql_catalog.hpp"

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/sql/iceberg_sql_commit.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog URI
//===--------------------------------------------------------------------===//

IcebergSqlCatalogUri IcebergSqlCatalogUri::Parse(const string &uri_p) {
	auto uri = uri_p;
	if (StringUtil::StartsWith(StringUtil::Lower(uri), "jdbc:")) {
		uri = uri.substr(5);
	}
	auto colon = uri.find(':');
	if (colon == string::npos) {
		throw InvalidConfigurationException(
		    "Unrecognized 'uri' (%s) for an Iceberg SQL catalog, expected e.g. 'sqlite:///<path>', "
		    "'postgresql://<host>/<database>' or 'duckdb:<path>'",
		    uri_p);
	}
	auto scheme = StringUtil::Lower(uri.substr(0, colon));
	// SQLAlchemy URIs may name a driver, e.g. 'postgresql+psycopg2://'
	auto driver = scheme.find('+');
	if (driver != string::npos) {
		scheme = scheme.substr(0, driver);
	}
	auto location = uri.substr(colon + 1);

	IcebergSqlCatalogUri result;
	if (scheme == "sqlite" || scheme == "duckdb") {
		result.storage_type = scheme;
		// SQLAlchemy spells a relative path 'sqlite:///<path>' and an absolute path 'sqlite:////<path>'
		if (StringUtil::StartsWith(location, "///")) {
			location = location.substr(3);
		} else if (StringUtil::StartsWith(location, "//")) {
			location = location.substr(2);
		}
		if (location.empty()) {
			throw InvalidConfigurationException("Missing the database path in the Iceberg SQL catalog 'uri' (%s)",
			                                    uri_p);
		}
		result.path = location;
	} else if (scheme == "postgresql" || scheme == "postgres") {
		result.storage_type = "postgres";
		result.path = "postgresql:" + location;
	} else {
		throw InvalidConfigurationException(
		    "Unsupported database '%s' in the Iceberg SQL catalog 'uri', supported are sqlite, postgresql and duckdb",
		    scheme);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

IcebergSqlCatalog::IcebergSqlCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                                     IcebergAttachOptions &attach_options_p, const optional<Identifier> &default_schema,
                                     IcebergSqlCatalogUri uri_p, string catalog_name_p, bool init_catalog_tables_p)
    : IcebergCatalog(IcebergCatalogType::SQL, db_p, access_mode, attach_options_p, default_schema),
      uri(std::move(uri_p)), catalog_name(std::move(catalog_name_p)), init_catalog_tables(init_catalog_tables_p),
      database_name("__iceberg_sql_" + db_p.GetName().GetIdentifierName()) {
	// The JDBC catalog separates nested namespaces with '.', table keys follow the stored namespace names
	namespace_separator = ".";
}

IcebergSqlCatalog::~IcebergSqlCatalog() = default;

unique_ptr<Connection> IcebergSqlCatalog::Connect() {
	return make_uniq<Connection>(GetDatabase());
}

//! ErrorData::Throw only preserves the exception type as a value; commits catch conflicts and constraint violations
//! (e.g. a concurrent write to the same catalog row) by their C++ type
[[noreturn]] static void ThrowQueryError(const ErrorData &error) {
	auto message = "Iceberg SQL catalog query failed: " + error.RawMessage();
	switch (error.Type()) {
	case ExceptionType::TRANSACTION:
		throw TransactionException(message);
	case ExceptionType::CONSTRAINT:
		throw ConstraintException(message);
	default:
		error.Throw("Iceberg SQL catalog query failed: ");
	}
}

vector<vector<Value>> IcebergSqlCatalog::Query(Connection &connection, const string &sql, vector<Value> parameters) {
	auto prepared = connection.Prepare(sql);
	if (prepared->HasError()) {
		ThrowQueryError(prepared->GetErrorObject());
	}
	auto result = prepared->Execute(parameters);
	if (result->HasError()) {
		ThrowQueryError(result->GetErrorObject());
	}
	vector<vector<Value>> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			vector<Value> values;
			for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
				values.push_back(chunk->GetValue(col, row));
			}
			rows.push_back(std::move(values));
		}
	}
	return rows;
}

string IcebergSqlCatalog::CatalogTable(const string &table_name) const {
	return KeywordHelper::WriteOptionallyQuoted(database_name) + "." + table_name;
}

string IcebergSqlCatalog::NamespaceName(const vector<string> &namespace_items) {
	return StringUtil::Join(namespace_items, ".");
}

void IcebergSqlCatalog::Initialize(ClientContext &context) {
	lock_guard<mutex> guard(initialize_lock);
	if (initialized) {
		return;
	}
	auto connection = Connect();
	const bool read_only = access_mode == AccessMode::READ_ONLY;
	if (uri.storage_type != "postgres") {
		// duckdb_databases() lists database files by their absolute path
		auto &fs = FileSystem::GetFileSystem(context);
		uri.path = fs.ExpandPath(uri.path);
		if (!fs.IsPathAbsolute(uri.path)) {
			uri.path = fs.JoinPath(FileSystem::GetWorkingDirectory(), uri.path);
		}
	}
	// A database can only be attached once, so Iceberg catalogs stored in the same database share its attachment
	auto attached = Query(*connection,
	                      "SELECT database_name FROM duckdb_databases() WHERE path = $1 AND type = $2 AND "
	                      "starts_with(database_name, '__iceberg_sql_') ORDER BY database_name LIMIT 1",
	                      {Value(uri.path), Value(uri.storage_type)});
	if (!attached.empty()) {
		database_name = attached[0][0].ToString();
	} else {
		auto attach = StringUtil::Format("ATTACH %s AS %s (TYPE %s%s)", KeywordHelper::WriteQuoted(uri.path),
		                                 KeywordHelper::WriteOptionallyQuoted(database_name), uri.storage_type,
		                                 read_only ? ", READ_ONLY" : "");
		Query(*connection, attach);
	}
	if (init_catalog_tables && !read_only) {
		// The V1 schema of the JDBC catalog, which PyIceberg's SqlCatalog also uses
		Query(*connection, StringUtil::Format("CREATE TABLE IF NOT EXISTS %s ("
		                                      "catalog_name VARCHAR(255) NOT NULL, "
		                                      "table_namespace VARCHAR(255) NOT NULL, "
		                                      "table_name VARCHAR(255) NOT NULL, "
		                                      "metadata_location VARCHAR(1000), "
		                                      "previous_metadata_location VARCHAR(1000), "
		                                      "iceberg_type VARCHAR(5), "
		                                      "PRIMARY KEY (catalog_name, table_namespace, table_name))",
		                                      CatalogTable(TABLES_TABLE)));
		Query(*connection, StringUtil::Format("CREATE TABLE IF NOT EXISTS %s ("
		                                      "catalog_name VARCHAR(255) NOT NULL, "
		                                      "namespace VARCHAR(255) NOT NULL, "
		                                      "property_key VARCHAR(255) NOT NULL, "
		                                      "property_value VARCHAR(1000), "
		                                      "PRIMARY KEY (catalog_name, namespace, property_key))",
		                                      CatalogTable(NAMESPACE_PROPERTIES_TABLE)));
	}
	// Catalogs created by the V0 schema of the JDBC catalog only store tables, and have no 'iceberg_type' column
	auto prepared = connection->Prepare(StringUtil::Format("SELECT * FROM %s LIMIT 0", CatalogTable(TABLES_TABLE)));
	if (prepared->HasError()) {
		prepared->GetErrorObject().Throw(StringUtil::Format("The Iceberg SQL catalog database '%s' has no catalog tables, "
		                                         "ATTACH with INIT_CATALOG_TABLES true to create them: ",
		                                         uri.path));
	}
	has_iceberg_type_column = false;
	for (auto &name : prepared->GetNames()) {
		if (StringUtil::CIEquals(name.GetIdentifierName(), "iceberg_type")) {
			has_iceberg_type_column = true;
		}
	}
	initialized = true;
}

void IcebergSqlCatalog::OnDetach(ClientContext &context) {
	lock_guard<mutex> guard(initialize_lock);
	if (!initialized) {
		return;
	}
	initialized = false;
	// Keep the catalog database attached while another Iceberg catalog uses it
	for (auto &database : DatabaseManager::Get(context).GetDatabases()) {
		if (database->IsSystem() || database->IsTemporary()) {
			continue;
		}
		auto &other = database->GetCatalog();
		if (&other == this || other.GetCatalogType() != "iceberg") {
			continue;
		}
		auto &other_iceberg = other.Cast<IcebergCatalog>();
		if (other_iceberg.GetIcebergCatalogType() == IcebergCatalogType::SQL &&
		    other_iceberg.Cast<IcebergSqlCatalog>().database_name == database_name) {
			return;
		}
	}
	auto connection = Connect();
	connection->Query(StringUtil::Format("DETACH DATABASE IF EXISTS %s",
	                                     KeywordHelper::WriteOptionallyQuoted(database_name)));
}

//===--------------------------------------------------------------------===//
// Reads
//===--------------------------------------------------------------------===//

bool IcebergSqlCatalog::SchemaExists(ClientContext &context, const string &schema_name) {
	Initialize(context);
	auto connection = Connect();
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT 1 FROM %s WHERE catalog_name = $1 AND namespace = $2 "
	                                     "UNION ALL SELECT 1 FROM %s WHERE catalog_name = $1 AND table_namespace = $2 "
	                                     "LIMIT 1",
	                                     CatalogTable(NAMESPACE_PROPERTIES_TABLE), CatalogTable(TABLES_TABLE)),
	                  {Value(catalog_name), Value(schema_name)});
	return !rows.empty();
}

IcebergListSchemasResult IcebergSqlCatalog::ListSchemas(ClientContext &context, const vector<string> &parent) {
	Initialize(context);
	auto connection = Connect();
	// A namespace exists if it has properties, or if a table refers to it
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT namespace FROM %s WHERE catalog_name = $1 "
	                                     "UNION SELECT table_namespace FROM %s WHERE catalog_name = $1 "
	                                     "ORDER BY 1",
	                                     CatalogTable(NAMESPACE_PROPERTIES_TABLE), CatalogTable(TABLES_TABLE)),
	                  {Value(catalog_name)});
	auto parent_prefix = parent.empty() ? string() : NamespaceName(parent) + ".";
	IcebergListSchemasResult result;
	for (auto &row : rows) {
		auto name = row[0].ToString();
		if (!StringUtil::StartsWith(name, parent_prefix)) {
			continue;
		}
		IRCAPISchema schema;
		schema.catalog_name = GetName().GetIdentifierName();
		schema.items = StringUtil::Split(name, '.');
		if (!attach_options.support_nested_namespaces && schema.items.size() > 1) {
			continue;
		}
		result.push_back(std::move(schema));
	}
	return result;
}

APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
IcebergSqlCatalog::LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) {
	Initialize(context);
	auto connection = Connect();
	// 'exists' only marks namespaces without other properties in the JDBC catalog
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT property_key, property_value FROM %s "
	                                     "WHERE catalog_name = $1 AND namespace = $2 AND property_key != 'exists'",
	                                     CatalogTable(NAMESPACE_PROPERTIES_TABLE)),
	                  {Value(catalog_name), Value(NamespaceName(schema.namespace_items))});
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>> result;
	result.status_ = HTTPStatusCode::OK_200;
	auto response = make_uniq<rest_api_objects::GetNamespaceResponse>();
	response->_namespace.value = schema.namespace_items;
	response->properties = case_insensitive_map_t<string>();
	for (auto &row : rows) {
		response->properties->emplace(row[0].ToString(), row[1].IsNull() ? string() : row[1].ToString());
	}
	// Like the JDBC catalog, report where the namespace's tables are created by default
	auto warehouse_root = GetWarehouse();
	if (!response->properties->count("location") && !warehouse_root.empty()) {
		response->properties->emplace("location", DefaultNamespaceLocation(schema.namespace_items));
	}
	result.result_ = std::move(response);
	return result;
}

IcebergListTablesResult IcebergSqlCatalog::ListTables(ClientContext &context, const vector<string> &namespace_items,
                                                      bool allow_forbidden) {
	Initialize(context);
	auto connection = Connect();
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT table_name FROM %s WHERE catalog_name = $1 AND table_namespace = $2%s "
	                                     "ORDER BY table_name",
	                                     CatalogTable(TABLES_TABLE),
	                                     has_iceberg_type_column ? " AND coalesce(iceberg_type, 'TABLE') = 'TABLE'" : ""),
	                  {Value(catalog_name), Value(NamespaceName(namespace_items))});
	vector<rest_api_objects::TableIdentifier> result;
	for (auto &row : rows) {
		rest_api_objects::TableIdentifier identifier;
		identifier._namespace.value = namespace_items;
		identifier.name = row[0].ToString();
		result.push_back(std::move(identifier));
	}
	return result;
}

IcebergLoadTableResult IcebergSqlCatalog::LoadTable(ClientContext &context, const vector<string> &namespace_items,
                                                    const string &table_name) {
	Initialize(context);
	auto connection = Connect();
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT metadata_location FROM %s "
	                                     "WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3%s",
	                                     CatalogTable(TABLES_TABLE),
	                                     has_iceberg_type_column ? " AND coalesce(iceberg_type, 'TABLE') = 'TABLE'" : ""),
	                  {Value(catalog_name), Value(NamespaceName(namespace_items)), Value(table_name)});
	IcebergLoadTableResult result;
	if (rows.empty() || rows[0][0].IsNull()) {
		result.status_ = HTTPStatusCode::NotFound_404;
		rest_api_objects::IcebergErrorResponse error;
		error._error.type = "NoSuchTableException";
		error._error.message = StringUtil::Format("Table does not exist: %s.%s", NamespaceName(namespace_items),
		                                          table_name);
		error._error.code = 404;
		result.error_ = std::move(error);
		return result;
	}
	auto metadata_location = rows[0][0].ToString();
	auto &fs = FileSystem::GetFileSystem(context);
	auto load_table_result = make_uniq<rest_api_objects::LoadTableResult>();
	load_table_result->metadata = IcebergTableMetadata::Parse(metadata_location, fs, "none");
	load_table_result->metadata_location = metadata_location;
	result.status_ = HTTPStatusCode::OK_200;
	result.result_ = std::move(load_table_result);
	return result;
}

bool IcebergSqlCatalog::SupportsViews(IcebergViewOperation operation) const {
	return false;
}

IcebergListViewsResult IcebergSqlCatalog::ListViews(ClientContext &context, const vector<string> &namespace_items) {
	return vector<rest_api_objects::TableIdentifier>();
}

IcebergLoadViewResult IcebergSqlCatalog::LoadView(ClientContext &context, const vector<string> &namespace_items,
                                                  const string &view_name) {
	throw NotImplementedException("Views are not supported by Iceberg SQL catalogs yet");
}

//===--------------------------------------------------------------------===//
// Statement time
//===--------------------------------------------------------------------===//

string IcebergSqlCatalog::DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
                                               const string &table_name) {
	// Like the JDBC catalog: the namespace's 'location' property, else '<warehouse>/<namespace levels>/<table>'
	Initialize(context);
	auto connection = Connect();
	auto rows = Query(*connection,
	                  StringUtil::Format("SELECT property_value FROM %s WHERE catalog_name = $1 AND namespace = $2 "
	                                     "AND property_key = 'location'",
	                                     CatalogTable(NAMESPACE_PROPERTIES_TABLE)),
	                  {Value(catalog_name), Value(NamespaceName(namespace_items))});
	if (!rows.empty() && !rows[0][0].IsNull() && !rows[0][0].ToString().empty()) {
		auto location = rows[0][0].ToString();
		StringUtil::RTrim(location, "/");
		return location + "/" + table_name;
	}
	if (GetWarehouse().empty()) {
		throw InvalidInputException("Cannot create table %s.%s without a 'location': the Iceberg SQL catalog has no "
		                            "warehouse (the ATTACH path) and the namespace has no 'location' property",
		                            NamespaceName(namespace_items), table_name);
	}
	return DefaultNamespaceLocation(namespace_items) + "/" + table_name;
}

string IcebergSqlCatalog::DefaultNamespaceLocation(const vector<string> &namespace_items) const {
	auto warehouse_root = GetWarehouse();
	StringUtil::RTrim(warehouse_root, "/");
	return warehouse_root + "/" + StringUtil::Join(namespace_items, "/");
}

IcebergTable &IcebergSqlCatalog::CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
                                             const IcebergCreateTableRequest &request) {
	auto location = request.GetLocation();
	if (location.empty()) {
		location = DefaultTableLocation(context, schema.namespace_items, request.GetName());
	}
	StringUtil::RTrim(location, "/");
	auto metadata = request.CreateInitialMetadata(UUID::ToString(UUID::GenerateRandomUUID()), location);
	auto &table_info = AddCreatedTable(context, IcebergTable(*this, schema, request.GetName(), std::move(metadata)));
	// The table is created from empty metadata at commit, like a table staged in a REST catalog
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	auto &transaction_data = table_info.GetOrCreateTransactionData(iceberg_transaction);
	transaction_data.TableAddAssertCreate();
	AddCreateTableUpdates(context, table_info);
	return table_info;
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//

void IcebergSqlCatalog::Commit(IcebergTransaction &transaction) {
	IcebergSqlTransactionCommit(*this, transaction).Commit();
}

} // namespace duckdb
