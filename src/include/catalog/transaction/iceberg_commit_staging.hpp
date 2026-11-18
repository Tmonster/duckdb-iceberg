#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

struct IcebergTable;
struct IcebergTransactionAlterUpdate;


//! A single table's commit request, with the manifest files written to build it
struct IcebergSingleTableStagedCommit {
	rest_api_objects::CommitTableRequest request;
	vector<string> created_metadata_files;
	bool retryable = false;
	IcebergRetryConfig retry_config;
};

//! The commit requests of several tables that have to be published atomically
struct IcebergMultiTableStagedCommit {
	rest_api_objects::CommitTransactionRequest request;
	vector<string> created_metadata_files;
	unordered_set<string> table_keys;
	bool retryable = false;
	IcebergRetryConfig retry_config;
};


//! Turns a transaction's buffered table updates into the requirements and updates of an Iceberg table commit,
//! writing the snapshots' manifest files and manifest lists along the way. This is independent of how the commit
//! is published: a REST catalog sends the result to the server, other catalogs apply it themselves.
struct IcebergCommitStaging {
	static IcebergSingleTableStagedCommit StageSingleTableCommit(DatabaseInstance &db, IcebergTable &table_info,
	                                                             ClientContext &context);
	static IcebergMultiTableStagedCommit StageMultiTableCommit(DatabaseInstance &db,
	                                                           IcebergTransactionAlterUpdate &alter_update,
	                                                           ClientContext &context);
	//! The only table with updates in 'alter_update', if any
	static optional_ptr<IcebergTable> GetSingleUpdatedTable(IcebergTransactionAlterUpdate &alter_update);
};

} // namespace duckdb
