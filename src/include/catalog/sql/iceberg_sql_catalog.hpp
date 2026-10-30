#pragma once

#include "duckdb/main/connection.hpp"

#include "catalog/iceberg_catalog.hpp"

namespace duckdb {

//! How the catalog database is reached, parsed from the 'uri' ATTACH option
struct IcebergSqlCatalogUri {
	//! The DuckDB storage type used to attach the database: 'duckdb', 'sqlite' or 'postgres'
	string storage_type;
	//! The path or connection string passed to ATTACH
	string path;

	//! Accepts the URIs of PyIceberg's SqlCatalog (SQLAlchemy) and the JDBC catalog of Iceberg Java:
	//! 'sqlite:///<path>', 'postgresql://...', 'jdbc:sqlite:<path>', 'jdbc:postgresql://...',
	//! and 'duckdb:<path>' for a DuckDB database file.
	static IcebergSqlCatalogUri Parse(const string &uri);
};

//! An Iceberg catalog stored in the tables of a SQL database, compatible with the JDBC catalog of Iceberg Java and
//! the SqlCatalog of PyIceberg. The database only stores a pointer to each table's current metadata file: DuckDB
//! writes the metadata files itself (see IcebergClientCommit) and swaps the pointers in one SQL transaction.
class IcebergSqlCatalog : public IcebergCatalog {
public:
	static constexpr const IcebergCatalogType TYPE = IcebergCatalogType::SQL;
	static constexpr const char *TABLES_TABLE = "iceberg_tables";
	static constexpr const char *NAMESPACE_PROPERTIES_TABLE = "iceberg_namespace_properties";

public:
	IcebergSqlCatalog(AttachedDatabase &db_p, AccessMode access_mode, IcebergAttachOptions &attach_options,
	                  const optional<Identifier> &default_schema, IcebergSqlCatalogUri uri, string catalog_name,
	                  bool init_catalog_tables);
	~IcebergSqlCatalog() override;

public:
	void OnDetach(ClientContext &context) override;

	bool SchemaExists(ClientContext &context, const string &schema_name) override;
	IcebergListSchemasResult ListSchemas(ClientContext &context, const vector<string> &parent) override;
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
	LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) override;
	IcebergListTablesResult ListTables(ClientContext &context, const vector<string> &namespace_items,
	                                   bool allow_forbidden) override;
	IcebergLoadTableResult LoadTable(ClientContext &context, const vector<string> &namespace_items,
	                                 const string &table_name) override;
	bool SupportsViews(IcebergViewOperation operation) const override;
	IcebergListViewsResult ListViews(ClientContext &context, const vector<string> &namespace_items) override;
	IcebergLoadViewResult LoadView(ClientContext &context, const vector<string> &namespace_items,
	                               const string &view_name) override;
	IcebergTable &CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
	                          const IcebergCreateTableRequest &request) override;
	void Commit(IcebergTransaction &transaction) override;

public:
	//! Attach the catalog database (once) and create the catalog tables if requested
	void Initialize(ClientContext &context);
	//! A new connection, for queries on the catalog database
	unique_ptr<Connection> Connect();
	//! Run a statement with parameters on 'connection', throwing on errors. Returns the result rows.
	static vector<vector<Value>> Query(Connection &connection, const string &sql, vector<Value> parameters = {});
	//! The qualified name of a table of the catalog database
	string CatalogTable(const string &table_name) const;
	//! The namespace stored for 'namespace_items', joined with '.' as the JDBC catalog does
	static string NamespaceName(const vector<string> &namespace_items);
	const string &GetCatalogName() const {
		return catalog_name;
	}
	//! The location for a new table that doesn't request one
	string DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
	                            const string &table_name);
	//! '<warehouse>/<namespace levels>', where tables of a namespace without a 'location' property are created
	string DefaultNamespaceLocation(const vector<string> &namespace_items) const;
	//! The 'iceberg_type' column was added to iceberg_tables by the V1 schema of the JDBC catalog
	bool HasIcebergTypeColumn() const {
		return has_iceberg_type_column;
	}

private:
	IcebergSqlCatalogUri uri;
	//! The value of the 'catalog_name' column, which lets several catalogs share one database
	string catalog_name;
	bool init_catalog_tables;
	//! The name the catalog database is attached as, shared by the Iceberg catalogs stored in the same database
	string database_name;

	mutex initialize_lock;
	bool initialized = false;
	bool has_iceberg_type_column = true;
};

} // namespace duckdb
