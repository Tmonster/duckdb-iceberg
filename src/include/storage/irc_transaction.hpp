
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

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

	void MarkTableAsDirty(const ICTableEntry &table);

public:
	optional_ptr<IRCSchemaSet> schemas;
	case_insensitive_set_t updated_schemas_tables;

	mutex updated_schemas_lock;
	//! Tables marked dirty in this transaction, to be rewritten on commit
	unordered_set<const ICTableEntry *> dirty_tables;

private:
	void CleanupFiles();

private:
	DatabaseInstance &db;
	IRCatalog &catalog;
	AccessMode access_mode;
};

} // namespace duckdb
