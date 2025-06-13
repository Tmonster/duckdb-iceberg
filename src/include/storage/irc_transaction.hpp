
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

enum class IRCTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class IRCTransaction : public Transaction {
public:
	IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IRCTransaction() override;

public:
	void Start();
	void Commit();
	void Rollback();
	static IRCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}
	optional_ptr<IRCSchemaSet> GetSchemas() {
		return schemas;
	}
	void UpdateSchemaTable(string schema_name, string table_name) {
		lock_guard<mutex> l(updated_schemas_lock);
		auto qualified_name = schema_name + "." + table_name;
		updated_schemas_tables.insert(qualified_name);
	}
	bool SchemaTableHasBeenUpdated(string schema_name, string table_name) {
		lock_guard<mutex> l(updated_schemas_lock);
		auto qualified_name = schema_name + "." + table_name;
		return updated_schemas_tables.find(qualified_name) != updated_schemas_tables.end();
	}

public:
	optional_ptr<IRCSchemaSet> schemas;
	case_insensitive_set_t updated_schemas_tables;

	IRCTransactionState transaction_state;
	AccessMode access_mode;

	mutex updated_schemas_lock;
};

} // namespace duckdb
