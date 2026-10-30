#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/enums/http_status_code.hpp"

#include "rest_catalog/objects/list.hpp"
#include "rest_catalog/objects/load_view_result.hpp"

namespace duckdb {

class IcebergCatalog;
class ClientContext;

//! Result and request types shared by every catalog type. They use the REST catalog's objects as the common
//! representation of catalog state; catalogs that are not backed by a REST server produce the same objects.
//! Requests are executed through the catalog's virtual hooks, so they can run on the IcebergRequestExecutor.

struct IRCAPISchema {
	//! The (potentially multiple) levels that the namespace is made up of
	vector<string> items;
	string catalog_name;
};

struct IRCAPICredentials {
	vector<rest_api_objects::StorageCredential> storage_credentials;
	//! Top-level config override used to build the refreshed secret; unset uses the existing table config.
	//! The LoadTable fallback currently supplies an empty override when config is omitted.
	optional<case_insensitive_map_t<string>> config;
};

enum class IRCEntryLookupStatus : uint8_t { EXISTS = 0, NOT_FOUND = 1, API_ERROR = 2 };

// Some API responses have error messages that need to be checked before being raised
// to the user, since sometimes is does not mean whole operation has failed.
// Ex: Glue will return an error when trying to get the metadata for a non-iceberg table during a list tables operation
//     The complete operation did not fail, just getting metadata for one table
template <typename T>
class APIResult {
public:
	APIResult() {};

	T result_;
	HTTPStatusCode status_;
	optional<rest_api_objects::IcebergErrorResponse> error_;
};

using IcebergLoadTableResult = APIResult<unique_ptr<const rest_api_objects::LoadTableResult>>;
using IcebergLoadViewResult = APIResult<unique_ptr<const rest_api_objects::LoadViewResult>>;

//! Owns the request inputs; execution only fetches and parses view metadata.
//! SQL interpretation and transaction publication remain the caller's responsibility.
class IcebergLoadViewRequest {
public:
	using Result = IcebergLoadViewResult;

	IcebergLoadViewRequest(vector<string> namespace_items, string view_name);
	IcebergLoadViewResult Execute(ClientContext &context, IcebergCatalog &catalog) const;

private:
	vector<string> namespace_items;
	string view_name;
};

//! Owns the request inputs; execution only fetches and parses a response.
//! The caller supplies a live context and catalog and owns cache lookup and result publication.
class IcebergLoadTableRequest {
public:
	using Result = IcebergLoadTableResult;

	IcebergLoadTableRequest(vector<string> namespace_items, string table_name);
	IcebergLoadTableResult Execute(ClientContext &context, IcebergCatalog &catalog) const;

private:
	vector<string> namespace_items;
	string table_name;
};

//! A refused table listing is distinct from a successful, empty listing.
using IcebergListTablesResult = optional<vector<rest_api_objects::TableIdentifier>>;
//! Schema listings retain already collected results if a subsequent page is refused.
using IcebergListSchemasResult = vector<IRCAPISchema>;
//! A refused view listing is distinct from an empty listing (including a missing namespace).
using IcebergListViewsResult = optional<vector<rest_api_objects::TableIdentifier>>;

//! Owns the namespace; execution fetches all pages without publishing view entries.
class IcebergListViewsRequest {
public:
	using Result = IcebergListViewsResult;

	explicit IcebergListViewsRequest(vector<string> namespace_items);
	IcebergListViewsResult Execute(ClientContext &context, IcebergCatalog &catalog) const;

private:
	vector<string> namespace_items;
};

//! Owns the namespace; execution fetches all pages without publishing catalog entries.
class IcebergListTablesRequest {
public:
	using Result = IcebergListTablesResult;

	explicit IcebergListTablesRequest(vector<string> namespace_items, bool allow_forbidden = true);
	IcebergListTablesResult Execute(ClientContext &context, IcebergCatalog &catalog) const;

private:
	vector<string> namespace_items;
	bool allow_forbidden;
};

//! Owns the parent namespace; execution includes pagination and configured nested-namespace traversal.
class IcebergListSchemasRequest {
public:
	using Result = IcebergListSchemasResult;

	explicit IcebergListSchemasRequest(vector<string> parent);
	IcebergListSchemasResult Execute(ClientContext &context, IcebergCatalog &catalog) const;

private:
	vector<string> parent;
};

} // namespace duckdb
