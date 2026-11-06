#pragma once

#include "duckdb/main/connection.hpp"

#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

class IcebergSqlCatalog;

//! A commit attempt on an Iceberg SQL catalog. A SQL database has transactions, so all schema and table changes of a
//! DuckDB transaction are written in one SQL transaction: a table's pointer is swapped with
//! 'UPDATE ... WHERE metadata_location = <the metadata the commit is based on>'.
class IcebergSqlMetastoreCommit : public IcebergMetastoreCommit {
public:
	IcebergSqlMetastoreCommit(IcebergSqlCatalog &catalog, unique_ptr<Connection> connection);
	~IcebergSqlMetastoreCommit() override;

public:
	bool IsAtomic() const override {
		return true;
	}
	bool NamespaceExists(const vector<string> &namespace_items) override;
	void CreateNamespace(const vector<string> &namespace_items) override;
	void UpdateNamespaceProperties(const vector<string> &namespace_items, const set<string> &removals,
	                               const case_insensitive_map_t<string> &updates) override;
	void DropNamespace(const vector<string> &namespace_items) override;
	string LoadMetadataLocation(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type) override;
	void CreateEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                 const string &metadata_location, optional_ptr<const IcebergTableMetadata> metadata) override;
	void SwapMetadataLocation(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                          const string &base_location, const string &new_location,
	                          optional_ptr<const IcebergTableMetadata> metadata) override;
	void DropEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	               optional_ptr<const IcebergTableMetadata> metadata) override;
	void RenameEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                 const string &new_name) override;
	void Finish() override;
	void Abort() override;

private:
	//! The 'iceberg_type' condition for 'type', empty for catalogs with the V0 schema
	string TypeCondition(IcebergMetastoreEntryType type) const;

private:
	IcebergSqlCatalog &catalog;
	unique_ptr<Connection> connection;
};

} // namespace duckdb
