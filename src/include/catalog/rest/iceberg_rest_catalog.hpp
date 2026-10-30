#pragma once

#include "catalog/iceberg_catalog.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

namespace rest_api_objects {
class CatalogConfig;
}

//! An Iceberg catalog managed by an Iceberg REST Catalog server
class IcebergRestCatalog : public IcebergCatalog {
public:
	static constexpr const IcebergCatalogType TYPE = IcebergCatalogType::REST;

public:
	explicit IcebergRestCatalog(AttachedDatabase &db_p, AccessMode access_mode,
	                            unique_ptr<IcebergAuthorization> auth_handler, IcebergAttachOptions &attach_options,
	                            const optional<Identifier> &default_schema);
	~IcebergRestCatalog() override;

public:
	void ParsePrefix();
	void Configure(ClientContext &context, const rest_api_objects::CatalogConfig &config,
	               IcebergEndpointType endpoint_type);
	IRCEndpointBuilder GetBaseUrl() const;
	void AddDefaultSupportedEndpoints();
	void AddS3TablesEndpoints();
	void AddGlueEndpoints();

public:
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
	void VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const override;
	void Commit(IcebergTransaction &transaction) override;

public:
	//! Whether the server supports atomic multi-table commits, and they are not disabled
	bool MultiTableCommitAvailable() const;
	//! Whether the table updates of 'alter_update' can be sent as one atomic multi-table commit
	bool CanUseMultiTableCommit(const IcebergTransactionAlterUpdate &alter_update) const;

public:
	unique_ptr<IcebergAuthorization> auth_handler;
	//! Base URI of the REST catalog
	string base_uri;
	//! version
	const string version;
	//! optional prefix path components
	vector<string> prefix;
	unordered_set<string> supported_urls;
};

} // namespace duckdb
