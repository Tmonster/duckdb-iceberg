#pragma once

#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

class IcebergGlueCatalog;

//! A commit attempt on an Iceberg Glue catalog. Glue has no transactions, so every operation is published as it is
//! called (IsAtomic() is false).
class IcebergGlueMetastoreCommit : public IcebergMetastoreCommit {
public:
	IcebergGlueMetastoreCommit(IcebergGlueCatalog &catalog, ClientContext &context);

public:
	bool IsAtomic() const override {
		return false;
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
	void Finish() override {
	}
	void Abort() override {
	}

public:
	//! The Hive type name Glue (and Athena) show for a column of 'type', as GlueCatalog of Iceberg Java writes it
	static string HiveTypeName(const LogicalType &type);

private:
	IcebergGlueCatalog &catalog;
	ClientContext &context;
};

} // namespace duckdb
