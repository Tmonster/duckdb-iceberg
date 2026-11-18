#pragma once

#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

class IcebergFilesystemCatalog;

//! A commit attempt on an Iceberg filesystem catalog. A table's pointer is its newest 'v<N>.metadata.json': the
//! catalog creates the next version exclusively (see IcebergFilesystemCatalog::WriteTableMetadata), and the version
//! hint written here only spares readers a search. Every operation is published as it is called.
class IcebergFilesystemMetastoreCommit : public IcebergMetastoreCommit {
public:
	IcebergFilesystemMetastoreCommit(IcebergFilesystemCatalog &catalog, ClientContext &context);

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

private:
	//! Point the version hint of a table at 'metadata_location'
	void WriteVersionHint(const IcebergMetastoreIdentifier &identifier, const string &metadata_location);

private:
	IcebergFilesystemCatalog &catalog;
	ClientContext &context;
};

} // namespace duckdb
