//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/iceberg_metadata_info.hpp"
#include "storage/iceberg_delete_filter.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

struct IcebergDeleteMap {

	void AddExtendedFileInfo(IcebergFileListExtendedEntry file_entry) {
		auto filename = file_entry.file.path;
		data_file_to_extended_file_info.emplace(std::move(filename), std::move(file_entry));
	}

	void AddToDeleteMap(const string filename, shared_ptr<IcebergDeleteData> delete_data) {
		delete_data_map.emplace(filename, delete_data);
	}

	IcebergFileListExtendedEntry GetExtendedFileInfo(const string &filename) {
		auto delete_entry = data_file_to_extended_file_info.find(filename);
		if (delete_entry == data_file_to_extended_file_info.end()) {
			throw InternalException("Could not find matching file for written delete file");
		}
		return delete_entry->second;
	}

	optional_ptr<IcebergDeleteData> GetDeleteData(const string &filename) {
		lock_guard<mutex> guard(lock);
		auto entry = delete_data_map.find(filename);
		if (entry == delete_data_map.end()) {
			return nullptr;
		}
		return entry->second.get();
	}

	void SetEntryAsModified(const string &data_filename) {
		lock_guard<mutex> guard(lock);
		// auto &manifest_entries =  data_file_to_manifest_file.find(filename);
		// D_ASSERT(manifest_file);
		// dirty_manifests.push_back(manifest_file);
		Printer::Print("given some delete file (-deletes.parquet), indicate the all entries in this manifest file need "
		               "to be written when the iceberg delete is finalized.");
	}

	unordered_set<IcebergManifestFile, IcebergManifestFileHash, IcebergManifestFileEq>
	GetManifestFilesWithModifiedEntries() {
		Printer::Print("Return list of manifest entries that are listed in a manifest file that has a "
		               "manifest_file_entry that has been updated");
		return dirty_manifests;
	}

private:
	mutex lock;
	// stores information about positional deletes
	unordered_map<string, shared_ptr<IcebergDeleteData>> delete_data_map;
	// data_file to extended file info
	// extended file info stores the delete file information.
	unordered_map<string, IcebergFileListExtendedEntry> data_file_to_extended_file_info;

	// delete manifest entries to write in the new Manifest File
	// unordered_set<optional_ptr<IcebergManifestEntry>> to_be_written_delete_entries;

	// delete file (i.e '-delete.[puffin|parquet]') to the extra manifest entry information.
	// FIXME: this may not be needed, you can use `delete_file_to_manifest_file` and the delete file
	//        string to find the ManifestEntry Information
	// unordered_map<string, vector<optional_ptr<IcebergManifestEntry>>> data_file_to_manifest_entry;
	// data file
	// delete file (i.e '-delete.[puffin|parquet]') to the manifest file information (which contains the manifest
	// entries)
	unordered_map<string, IcebergManifestFile> data_file_to_manifest_file;

	// a map of a IcebergManifestEntry to the Iceberg Manifest it is listed in.
	// unordered_map<IcebergManifestEntry, IcebergManifest> delete_manifest_entry_to_manifest_file;

	// need to know which IcebergManifestFiles need to be written in the delete update
	unordered_set<IcebergManifestFile, IcebergManifestFileHash, IcebergManifestFileEq> dirty_manifests;
};

struct WrittenColumnInfo {
	WrittenColumnInfo() = default;
	WrittenColumnInfo(LogicalType type_p, int32_t field_id) : type(std::move(type_p)), field_id(field_id) {
	}

	LogicalType type;
	int32_t field_id;
};

class IcebergDeleteLocalState : public LocalSinkState {
public:
	string current_file_name;
	vector<idx_t> file_row_numbers;
};

class IcebergDeleteGlobalState : public GlobalSinkState {
public:
	explicit IcebergDeleteGlobalState() {
		written_columns["file_path"] = WrittenColumnInfo(LogicalType::VARCHAR, MultiFileReader::FILENAME_FIELD_ID);
		written_columns["pos"] = WrittenColumnInfo(LogicalType::BIGINT, MultiFileReader::ORDINAL_FIELD_ID);
	}

	mutex lock;
	unordered_map<string, IcebergDeleteFileInfo> written_files;
	unordered_map<string, WrittenColumnInfo> written_columns;
	idx_t total_deleted_count = 0;
	unordered_map<string, vector<idx_t>> deleted_rows;
	unordered_set<string> filenames;

	void Flush(IcebergDeleteLocalState &local_state) {
		auto &local_entry = local_state.file_row_numbers;
		if (local_entry.empty()) {
			return;
		}
		lock_guard<mutex> guard(lock);
		auto &global_entry = deleted_rows[local_state.current_file_name];
		global_entry.insert(global_entry.end(), local_entry.begin(), local_entry.end());
		total_deleted_count += local_entry.size();
		local_entry.clear();
	}

	void FinalFlush(IcebergDeleteLocalState &local_state) {
		Flush(local_state);
		// flush the file names to the global state
		lock_guard<mutex> guard(lock);
		filenames.emplace(local_state.current_file_name);
	}
};

class IcebergDelete : public PhysicalOperator {
public:
	IcebergDelete(PhysicalPlan &physical_plan, ICTableEntry &table, PhysicalOperator &child,
	              shared_ptr<IcebergDeleteMap> delete_map_p, vector<idx_t> row_id_indexes);

	//! The table to delete from
	ICTableEntry &table;
	// A map of filename -> data file index and filename -> delete data
	shared_ptr<IcebergDeleteMap> delete_map;
	//! The column indexes for the relevant row-id columns
	vector<idx_t> row_id_indexes;

public:
	// // Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, ICTableEntry &table,
	                                    PhysicalOperator &child_plan, vector<idx_t> row_id_indexes);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	void WritePositionalDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state,
	                               const string &filename, IcebergDeleteFileInfo delete_file,
	                               set<idx_t> sorted_deletes) const;
	void FlushDelete(IRCTransaction &transaction, ClientContext &context, IcebergDeleteGlobalState &global_state,
	                 const string &filename, vector<idx_t> &deleted_rows) const;
};

} // namespace duckdb
