#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

class IcebergSqlCatalog;
class IcebergTransaction;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;
struct IcebergTransactionRenameUpdate;
struct IcebergRetryConfig;

//! Publishes a transaction's buffered changes to an Iceberg SQL catalog. Unlike the REST protocol, a SQL database
//! has transactions, so all schema and table changes of a DuckDB transaction are written atomically: every table
//! commit writes a new metadata file (see IcebergClientCommit) and swaps the table's pointer with
//! 'UPDATE ... WHERE metadata_location = <the metadata the commit is based on>'.
class IcebergSqlTransactionCommit {
public:
	IcebergSqlTransactionCommit(IcebergSqlCatalog &catalog, IcebergTransaction &transaction);

public:
	void Commit();

private:
	void CommitWithRetries(ClientContext &context);
	void WriteSchemaCreates(Connection &connection);
	void WriteSchemaPropertyUpdates(Connection &connection);
	void WriteTableUpdates(Connection &connection, IcebergTransactionAlterUpdate &alter_update, ClientContext &context,
	                       IcebergRetryConfig &retry_config, bool &retryable, unordered_set<string> &table_keys);
	void WriteTableDelete(Connection &connection, IcebergTransactionDeleteUpdate &delete_update,
	                      ClientContext &context);
	void WriteTableRename(Connection &connection, IcebergTransactionRenameUpdate &rename_update,
	                      ClientContext &context);
	void WriteViewDeletes(Connection &connection);
	void WriteViewCreates(Connection &connection, ClientContext &context);
	void WriteSchemaDeletes(Connection &connection);
	//! Remove the metadata files written by a commit attempt that did not succeed
	void RemoveWrittenFiles(ClientContext &context);

private:
	IcebergSqlCatalog &catalog;
	IcebergTransaction &transaction;
	DatabaseInstance &db;
	//! Metadata files (manifests, manifest lists, metadata.json) written by the current attempt
	vector<string> written_files;
	//! Updates of the in-memory catalog state, applied once the SQL transaction committed
	vector<std::function<void()>> on_commit;
};

} // namespace duckdb
