#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! An error response of the Glue API, e.g. EntityNotFoundException
class IcebergGlueException : public Exception {
public:
	IcebergGlueException(const string &error_type, const string &message);

	//! The '__type' of the error without its namespace, e.g. 'EntityNotFoundException'
	string error_type;
};

//! The settings to call the AWS Glue API with
struct IcebergGlueClientOptions {
	//! The Glue Data Catalog id, empty for the default catalog of the account
	string catalog_id;
	string region;
	//! Overrides the endpoint, e.g. 'http://127.0.0.1:5000' for moto
	string endpoint;
	//! The name of the S3/AWS secret holding the credentials, empty for the default secret
	string secret_name;
};

//! Calls the AWS Glue API (the AWS JSON 1.1 protocol): a POST of a JSON body with 'X-Amz-Target: AWSGlue.<operation>',
//! signed with SigV4 and sent over DuckDB's HTTP stack, so this needs nothing from aws-sdk-cpp.
class IcebergGlueClient {
public:
	explicit IcebergGlueClient(IcebergGlueClientOptions options);

public:
	//! Call 'operation' with 'request' (an object, CatalogId is added) and return the parsed response. Throws an
	//! IcebergGlueException for an error response.
	unique_ptr<JSONDocument> Call(ClientContext &context, const string &operation, JSONWriter &request,
	                              JSONMutableValue request_body);
	const IcebergGlueClientOptions &GetOptions() const {
		return options;
	}

private:
	IcebergGlueClientOptions options;
};

} // namespace duckdb
