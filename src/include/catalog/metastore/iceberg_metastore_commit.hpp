#pragma once

#include "duckdb/main/client_context.hpp"

#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

class IcebergTransaction;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;
struct IcebergTransactionRenameUpdate;
struct IcebergRetryConfig;

//! Publishes a transaction's buffered changes to a metastore catalog. Every table commit applies the transaction's
//! updates to the table's current metadata, writes a new metadata file (see IcebergClientCommit), and swaps the
//! table's pointer in the metastore if it still points at the metadata the commit is based on. A conflict retries the
//! commit on the table's new metadata, unless a change of the attempt may be visible already.
class IcebergMetastoreTransactionCommit {
public:
	IcebergMetastoreTransactionCommit(IcebergMetastoreCatalog &catalog, IcebergTransaction &transaction);

public:
	void Commit();

private:
	void CommitWithRetries(ClientContext &context);
	void CreateNamespaces(IcebergMetastoreCommit &commit);
	void UpdateNamespaceProperties(IcebergMetastoreCommit &commit);
	void CommitTables(IcebergMetastoreCommit &commit, IcebergTransactionAlterUpdate &alter_update,
	                  ClientContext &context, IcebergRetryConfig &retry_config, bool &retryable,
	                  unordered_set<string> &table_keys);
	void DropTable(IcebergMetastoreCommit &commit, IcebergTransactionDeleteUpdate &delete_update,
	               ClientContext &context);
	void RenameTable(IcebergMetastoreCommit &commit, IcebergTransactionRenameUpdate &rename_update);
	void DropViews(IcebergMetastoreCommit &commit);
	void CreateViews(IcebergMetastoreCommit &commit, ClientContext &context);
	void DropNamespaces(IcebergMetastoreCommit &commit);
	//! Remove the metadata files written by a commit attempt that did not succeed
	void RemoveWrittenFiles(ClientContext &context);
	//! Mark the attempt published after a table or view change of a metastore without transactions
	void OnEntryChanged(IcebergMetastoreCommit &commit);
	vector<string> SplitNamespace(const string &schema_name) const;

private:
	IcebergMetastoreCatalog &catalog;
	IcebergTransaction &transaction;
	DatabaseInstance &db;
	//! Metadata files (manifests, manifest lists, metadata.json) written by the current attempt
	vector<string> written_files;
	//! Updates of the in-memory catalog state, applied once the attempt is published
	vector<std::function<void()>> on_commit;
};

} // namespace duckdb
