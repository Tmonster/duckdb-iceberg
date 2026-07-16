
#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "catalog/rest/storage/iceberg_authorization.hpp"

namespace duckdb {

class AWSInput {
public:
	AWSInput(AttachedDatabase &db) : attached_db(db) {
	}

public:
	unique_ptr<HTTPResponse> Request(RequestType request_type, ClientContext &context, HTTPHeaders &headers,
	                                 const string &data);

	unique_ptr<HTTPResponse> ExecuteRequest(ClientContext &context, RequestType request_type, HTTPHeaders &headers,
	                                        const string &body = "");

private:
	string GetURLEncodedPath() const;
	string GetQueryString() const;

public:
	AttachedDatabase &attached_db;
	//! The scheme to use for this request (http or https), defaults to https
	string scheme = "https";
	string authority;
	vector<string> path_segments;
	vector<std::pair<string, string>> query_string_parameters;
	string user_agent;
	string cert_path;
	bool use_httpfs_timeout = false;
	idx_t request_timeout_in_ms;

	//! Provider credentials
	string key_id;
	string secret;
	string session_token;
	//! Signer input
	string service;
	string region;
};

} // namespace duckdb
