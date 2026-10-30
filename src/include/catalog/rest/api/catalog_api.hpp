
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"

#include "catalog/rest/api/url_utils.hpp"
#include "iceberg_attach.hpp"
#include "rest_catalog/objects/list.hpp"
#include "rest_catalog/objects/load_view_result.hpp"
#include "catalog/iceberg_catalog_requests.hpp"

namespace duckdb {

class IcebergRestCatalog;
struct IcebergCreateTableRequest;
class IcebergSchemaEntry;
class IcebergTableSchemaVersion;

class CommitResult {
public:
	CommitResult() {
	}

public:
	bool Success() const {
		return success;
	}
	bool IsConflict() const {
		return status == HTTPStatusCode::Conflict_409;
	}
	void Throw(const string &url) const;

public:
	bool success = false;
	HTTPStatusCode status = HTTPStatusCode::OK_200;
	string reason;
	string body;
	HTTPHeaders headers;
	optional<rest_api_objects::IcebergErrorResponse> error_;
};

class IRCAPI {
public:
	static const string API_VERSION_1;
	//! Returns 'nullopt' if the catalog refused the listing, which must not be read as "the schema is empty".
	static IcebergListTablesResult GetTables(ClientContext &context, IcebergRestCatalog &catalog,
	                                         const IcebergSchemaEntry &schema);
	static bool VerifyResponse(ClientContext &context, IcebergRestCatalog &catalog, IRCEndpointBuilder &url_builder,
	                           bool execute_head);
	static bool VerifySchemaExistence(ClientContext &context, IcebergRestCatalog &catalog, const string &schema);
	static bool VerifyTableExistence(ClientContext &context, IcebergRestCatalog &catalog, const IcebergSchemaEntry &schema,
	                                 const string &table);
	static vector<string> ParseSchemaName(const string &namespace_name);
	static IcebergLoadTableResult LoadTable(ClientContext &context, IcebergRestCatalog &catalog,
	                                        const vector<string> &namespace_items, const string &table_name);
	//! Returns 'nullopt' if the catalog refused the listing and 'allow_forbidden' is set.
	static IcebergListTablesResult ListTables(ClientContext &context, IcebergRestCatalog &catalog,
	                                          const vector<string> &namespace_items, bool allow_forbidden);
	//! Includes pagination and configured nested-namespace traversal.
	static IcebergListSchemasResult ListSchemas(ClientContext &context, IcebergRestCatalog &catalog,
	                                            const vector<string> &parent);
	static IcebergListViewsResult ListViews(ClientContext &context, IcebergRestCatalog &catalog,
	                                        const vector<string> &namespace_items);
	static IcebergLoadViewResult LoadView(ClientContext &context, IcebergRestCatalog &catalog,
	                                      const vector<string> &namespace_items, const string &view_name);
	static IcebergLoadTableResult GetTable(ClientContext &context, IcebergRestCatalog &catalog,
	                                       const IcebergSchemaEntry &schema, const string &table_name);
	static APIResult<IRCAPICredentials> GetTableCredentials(ClientContext &context, IcebergRestCatalog &catalog,
	                                                        const IcebergSchemaEntry &schema, const string &table_name);
	static APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
	GetNamespace(ClientContext &context, IcebergRestCatalog &catalog, const IcebergSchemaEntry &schema);
	static IcebergListSchemasResult GetSchemas(ClientContext &context, IcebergRestCatalog &catalog,
	                                           const vector<string> &parent);
	static CommitResult CommitTableUpdate(ClientContext &context, IcebergRestCatalog &catalog, const vector<string> &schema,
	                                      const string &table_name, const string &body);
	static void CommitTableDelete(ClientContext &context, IcebergRestCatalog &catalog, const vector<string> &schema,
	                              const string &table_name);
	static void CommitTableRename(ClientContext &context, IcebergRestCatalog &catalog, const string &body);
	static CommitResult CommitMultiTableUpdate(ClientContext &context, IcebergRestCatalog &catalog, const string &body);
	//! returns false if the namespace already exists
	static bool CommitNamespaceCreate(ClientContext &context, IcebergRestCatalog &catalog, const string &schema,
	                                  string body);
	static void CommitNamespaceDrop(ClientContext &context, IcebergRestCatalog &catalog,
	                                const vector<string> &namespace_items);
	static void CommitNamespacePropertiesUpdate(ClientContext &context, IcebergRestCatalog &catalog, string body,
	                                            const vector<string> &namespace_items);
	//! stage create = false, table is created immediately in the IRC
	//! stage create = true, table is not created, but metadata is initialized and returned
	static rest_api_objects::LoadTableResult CommitNewTable(ClientContext &context, IcebergRestCatalog &catalog,
	                                                        const vector<string> &namespace_items,
	                                                        const IcebergCreateTableRequest &request);
	static rest_api_objects::CatalogConfig GetCatalogConfig(ClientContext &context, IcebergRestCatalog &catalog,
	                                                        const string &warehouse);

	//! View operations
	static IcebergListViewsResult GetViews(ClientContext &context, IcebergRestCatalog &catalog,
	                                       const IcebergSchemaEntry &schema);
	static IcebergLoadViewResult GetView(ClientContext &context, IcebergRestCatalog &catalog,
	                                     const IcebergSchemaEntry &schema, const string &view_name);
	static void CommitNewView(ClientContext &context, IcebergRestCatalog &catalog, const IcebergSchemaEntry &schema,
	                          const string &json_body);
	static void CommitViewDelete(ClientContext &context, IcebergRestCatalog &catalog, const vector<string> &schema,
	                             const string &view_name);
};

} // namespace duckdb
