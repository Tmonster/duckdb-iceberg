
#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread_annotation.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"

namespace duckdb {
struct CreateTableInfo;
struct DropInfo;
class IcebergSchemaEntry;
class IcebergTransaction;

//! Outcome of re-resolving one table in IcebergTableSet::RefreshAll.
struct IcebergTableRefreshResult {
	string table_name;
	bool refreshed = false;
	string error;
};

class IcebergTableSet {
public:
	explicit IcebergTableSet(IcebergSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void ScanTables(ClientContext &context, const std::function<void(IcebergTable &)> &callback);
	void DropEntry(ClientContext &context, DropInfo &info, bool delete_entry);
	void RenameEntry(const string &name, const string &new_name, IcebergTable &&new_table);
	void InvalidateEntry(const string &name);
	//! Re-list this schema and re-resolve every table in it, discarding what was resolved before.
	//! Each table is replaced rather than refilled, so a query still holding the previous version is
	//! unaffected. One row is appended to 'results' per table, including the ones that failed.
	void RefreshAll(ClientContext &context, vector<IcebergTableRefreshResult> &results);
	static IcebergTable &CreateNewEntry(ClientContext &context, IcebergCatalog &catalog, IcebergSchemaEntry &schema,
	                                    CreateTableInfo &info);

private:
	IcebergTableSchemaVersion &GetOrCreateDummy(IcebergTable &table_info) const DUCKDB_REQUIRES(entry_lock);
	void LoadEntriesInternal(ClientContext &context) DUCKDB_REQUIRES(entry_lock);
	shared_ptr<IcebergTable> CreateEntryInternal(const string &name, IcebergTable &&table,
	                                             shared_ptr<IcebergTable> &old_entry) DUCKDB_REQUIRES(entry_lock);

public:
	//! return true if request to LoadTableInformation was successful and entry has been filled
	//! or if entry is already filled. Returns False otherwise
	bool FillEntry(ClientContext &context, IcebergTable &table);

public:
	IcebergSchemaEntry &schema;
	Catalog &catalog;

private:
	annotated_mutex entry_lock;
	case_insensitive_map_t<shared_ptr<IcebergTable>> entries DUCKDB_GUARDED_BY(entry_lock);
};

} // namespace duckdb
