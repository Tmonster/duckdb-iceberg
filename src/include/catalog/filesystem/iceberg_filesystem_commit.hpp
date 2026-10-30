#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {

class IcebergFilesystemCatalog;
class IcebergTransaction;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;
struct IcebergRetryConfig;

//! Publishes a transaction's buffered changes to an Iceberg filesystem catalog. A table commit applies the
//! transaction's updates client-side (see IcebergClientCommit) and creates the next version's metadata file
//! exclusively, which is the commit point; the version hint is updated afterwards. Namespace changes are applied
//! one after another, not atomically.
class IcebergFilesystemTransactionCommit {
public:
	IcebergFilesystemTransactionCommit(IcebergFilesystemCatalog &catalog, IcebergTransaction &transaction);

public:
	void Commit();

private:
	void CommitWithRetries(ClientContext &context);
	void CreateNamespaces(ClientContext &context);
	void CommitTables(IcebergTransactionAlterUpdate &alter_update, ClientContext &context,
	                  IcebergRetryConfig &retry_config, bool &retryable, unordered_set<string> &table_keys);
	void DropTable(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context);
	void DropNamespaces(ClientContext &context);

private:
	IcebergFilesystemCatalog &catalog;
	IcebergTransaction &transaction;
	DatabaseInstance &db;
	//! Files written by the current attempt of a table commit that did not succeed
	vector<string> written_files;
	//! Whether a change was published already, after which a failure must not remove the transaction's files
	bool published = false;
	//! Updates of the in-memory catalog state, applied once all changes are published
	vector<std::function<void()>> on_commit;
};

} // namespace duckdb
