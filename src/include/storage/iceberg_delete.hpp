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

struct IcebergDeleteData {
	// the rows deleted from that file after reading all the positional delete data
	unordered_set<int64_t> deleted_rows;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) const;
};

struct IcebergFileListExtendedEntry {
	// the parquet file with the data
	string data_file_name;

	// the actual delete data (i.e deleted rows)
	shared_ptr<IcebergDeleteData> delete_data;

	// the parquet file(s) with the delete data. Multiple positional delete files can refer to one data file.
	unordered_set<string> old_delete_file_names;

	// new delete file name (if a delete operation is occuring).
	string new_delete_file_name;

	IcebergFileData file;
	IcebergDeleteFileInfo delete_file;
	idx_t row_count;
	idx_t delete_count = 0;
};


struct IcebergDeleteMap {

	void AddExtendedFileInfo(IcebergFileListExtendedEntry file_entry) {
		auto filename = file_entry.file.path;
		// if (filename == "s3://warehouse/default/one_delete_file_multiple_data_files/data/00000-8-4a11d8ac-edf0-4d2d-939c-3e20bf3d5482-00001.parquet") {
		// 	auto break_here = 0;
		// }
		data_file_to_extended_file_info.emplace(std::move(filename), std::move(file_entry));
	}

	void AddDeleteFileNameToDeleteDataEntry(string &data_file_name, const string &delete_file_name) {
		if (data_file_to_extended_file_info.find(data_file_name) == data_file_to_extended_file_info.end()) {
			// a delete file has delete infromation for a data file we are not scanning.
			// add it to our delete map anyway, and later on we will make a delete file for it
			data_file_to_extended_file_info[data_file_name] = IcebergFileListExtendedEntry();
		}
		data_file_to_extended_file_info[data_file_name].old_delete_file_names.insert(delete_file_name);
	}

	// for a datafile, add the new delete file name to the extended file info
	void AddNewDeleteFileName(const string &data_file_name, const string &new_delete_file_name) {
		if (data_file_to_extended_file_info.find(data_file_name) == data_file_to_extended_file_info.end()) {
			throw InternalException("Could not find delete map entry for data file %s", data_file_name.c_str());
		}
		auto &extended_file_info = data_file_to_extended_file_info[data_file_name];
		extended_file_info.new_delete_file_name = new_delete_file_name;
	}

	void AddToDeleteMap(const string filename, shared_ptr<IcebergDeleteData> delete_data) {
		if (data_file_to_extended_file_info.find(filename) == data_file_to_extended_file_info.end()) {
			throw InternalException("We should not be here 2");
		}
		auto &extended_delete_info = data_file_to_extended_file_info.find(filename)->second;
		Printer::Print("check if positional delete data is merged. It should be");
		extended_delete_info.delete_data = delete_data;
	}

	IcebergFileListExtendedEntry &GetExtendedFileInfo(const string &filename) {
		auto extended_file_info = data_file_to_extended_file_info.find(filename);
		if (extended_file_info == data_file_to_extended_file_info.end()) {
			throw InternalException("Could not find matching file for written delete file");
		}
		return extended_file_info->second;
	}

	optional_ptr<IcebergDeleteData> GetDeleteData(const string &filename) {
		lock_guard<mutex> guard(lock);
		auto entry = data_file_to_extended_file_info.find(filename);
		if (entry == data_file_to_extended_file_info.end()) {
			throw InternalException("Could not find matching file for deleted data");
			return nullptr;
		}
		return entry->second.delete_data;
	}

	void AddPosDeleteInfo(string positional_delete_filename, string data_file_name) {
		positional_delete_file_to_data_files[positional_delete_filename].insert(data_file_name);
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

	void GetStaleDataFiles(const string &pos_delete_filename, unordered_set<string> &result) {
		auto data_files_in_pos_delete_file = positional_delete_file_to_data_files.find(pos_delete_filename);
		if (data_files_in_pos_delete_file == positional_delete_file_to_data_files.end()) {
			throw InternalException("why is there no data here");
		}
		for (auto &data_file : data_files_in_pos_delete_file->second) {
			auto extended_info = data_file_to_extended_file_info.find(data_file);
			if (extended_info == data_file_to_extended_file_info.end()) {
				throw InternalException("Could not find matching file for reading data file");
			}
			if (extended_info->second.new_delete_file_name.empty()) {
				result.insert(data_file);
			}
		}
	}

	unordered_set<string> GetDataFilesThatNeedANewDeleteFile() {
		unordered_set<string> result;
		for (auto &entry : dirty_data_files) {
			auto extended_delete_info = data_file_to_extended_file_info.find(entry);
			if (extended_delete_info == data_file_to_extended_file_info.end()) {
				throw InternalException("we should not be here");
			}
			auto pos_delete_files = extended_delete_info->second.old_delete_file_names;
			for (auto &pos_delete_file : pos_delete_files) {
				GetStaleDataFiles(pos_delete_file, result);
			}
		}
		return result;
	}

	void MarkDataFileDirty(const string &data_file) {
		dirty_data_files.insert(data_file);
	}

private:
	mutex lock;

	// data_file to extended file info
	// extended file info stores the delete file information.
	unordered_map<string, IcebergFileListExtendedEntry> data_file_to_extended_file_info;

	// positional delete file name to data_file_names
	unordered_map<string, unordered_set<string>> positional_delete_file_to_data_files;

	// all data files that have a new delete file from the delete operation
	// This is used to find all data files that do not have new deletes, but because their deletes
	// are in a pos_delete file with other data files, we need to write a new delete file for them
	unordered_set<string> dirty_data_files;


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
		filenames.insert(local_state.current_file_name);
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
