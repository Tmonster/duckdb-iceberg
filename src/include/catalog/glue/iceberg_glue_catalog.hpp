#pragma once

#include "catalog/glue/iceberg_glue_client.hpp"
#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

//! An Iceberg catalog stored in the AWS Glue Data Catalog through the Glue API, like GlueCatalog of Iceberg Java (and
//! not through Glue's Iceberg REST endpoint). A Glue database is a namespace, and a Glue table with the parameter
//! 'table_type' = 'ICEBERG' is an Iceberg table whose parameter 'metadata_location' points at its current metadata
//! file. DuckDB writes the metadata files itself and swaps the pointer with UpdateTable, using the table's VersionId
//! to detect concurrent changes.
//!
//! Glue can't change several tables atomically, so a transaction can only change one table.
class IcebergGlueCatalog : public IcebergMetastoreCatalog {
public:
	static constexpr const IcebergCatalogType TYPE = IcebergCatalogType::GLUE;
	static constexpr const char *TABLE_TYPE_PARAMETER = "table_type";
	static constexpr const char *ICEBERG_TABLE_TYPE = "ICEBERG";
	static constexpr const char *METADATA_LOCATION_PARAMETER = "metadata_location";
	static constexpr const char *PREVIOUS_METADATA_LOCATION_PARAMETER = "previous_metadata_location";

public:
	IcebergGlueCatalog(AttachedDatabase &db_p, AccessMode access_mode, IcebergAttachOptions &attach_options,
	                   const optional<Identifier> &default_schema, IcebergGlueClientOptions client_options);
	~IcebergGlueCatalog() override;

public:
	bool SchemaExists(ClientContext &context, const string &schema_name) override;
	IcebergListSchemasResult ListSchemas(ClientContext &context, const vector<string> &parent) override;
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
	LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) override;
	IcebergListTablesResult ListTables(ClientContext &context, const vector<string> &namespace_items,
	                                   bool allow_forbidden) override;
	bool SupportsViews(IcebergViewOperation operation) const override;
	IcebergListViewsResult ListViews(ClientContext &context, const vector<string> &namespace_items) override;
	void VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const override;

	string MetastoreName() const override {
		return "Glue catalog";
	}
	string GetMetadataLocation(ClientContext &context, const IcebergMetastoreIdentifier &identifier,
	                           IcebergMetastoreEntryType type) override;
	string DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
	                            const string &table_name) override;
	unique_ptr<IcebergMetastoreCommit> BeginCommit(ClientContext &context) override;

public:
	IcebergGlueClient &GetClient() {
		return client;
	}
	//! The Glue database of a namespace, Glue has no nested namespaces
	static string DatabaseName(const vector<string> &namespace_items);
	//! '<warehouse>/<database>.db', where the tables of a database without a LocationUri are created
	string DefaultDatabaseLocation(const string &database_name) const;
	//! GetDatabase, nullptr if the database doesn't exist
	unique_ptr<JSONDocument> GetDatabase(ClientContext &context, const string &database_name);
	//! GetTable, nullptr if the table doesn't exist
	unique_ptr<JSONDocument> GetTable(ClientContext &context, const IcebergMetastoreIdentifier &identifier);
	//! Calls a paginated Glue listing until it has no NextToken
	void ListPages(ClientContext &context, const string &operation, const string &list_key,
	               const std::function<void(JSONWriter &, JSONMutableValue &)> &build_request,
	               const std::function<void(JSONValue)> &callback);
	//! Whether a Glue table is an Iceberg table
	static bool IsIcebergTable(JSONValue table);
	//! The value of a Glue table's or database's parameter, empty if it isn't set
	static string Parameter(JSONValue entry, const string &key);

private:
	IcebergGlueClient client;
};

} // namespace duckdb
