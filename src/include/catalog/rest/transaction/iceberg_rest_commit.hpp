#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {

class IcebergRestCatalog;
class IcebergTransaction;
struct IcebergTransactionAlterUpdate;
struct IcebergTransactionDeleteUpdate;
struct IcebergTransactionRenameUpdate;

//! Publishes a transaction's buffered changes to an Iceberg REST Catalog. The REST protocol has no
//! multi-object transactions, so schema, table and view changes are sent as separate requests.
class IcebergRestTransactionCommit {
public:
	IcebergRestTransactionCommit(IcebergRestCatalog &catalog, IcebergTransaction &transaction);

public:
	void Commit();

private:
	void DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context);
	void DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context);
	void DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context);
	void DoSchemaCreates(ClientContext &context);
	void DoSchemaDeletes(ClientContext &context);
	void DoSchemaPropertyUpdates(ClientContext &context);
	void DoViewCreates(ClientContext &context);
	void DoViewDeletes(ClientContext &context);

private:
	IcebergRestCatalog &catalog;
	IcebergTransaction &transaction;
	DatabaseInstance &db;
};

} // namespace duckdb
