#include "deletes/positional_delete.hpp"
#include "iceberg_multi_file_list.hpp"
#include "storage/iceberg_delete.hpp"

namespace duckdb {

void IcebergMultiFileList::ScanPositionalDeleteFile(DataChunk &result, const string &delete_file_name, shared_ptr<IcebergDeleteMap> delete_map) const {
	//! FIXME: might want to check the 'columns' of the 'reader' to check, field-ids are:
	auto names = FlatVector::GetData<string_t>(result.data[0]);  //! 2147483546
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]); //! 2147483545

	auto count = result.size();
	if (count == 0) {
		return;
	}
	reference<string_t> current_file_path = names[0];

	auto initial_key = current_file_path.get().GetString();
	auto it = positional_delete_data.find(initial_key);
	if (it == positional_delete_data.end()) {
		it =
		    positional_delete_data.emplace(initial_key, make_uniq<IcebergPositionalDeleteData>(initial_key)).first;
	}
	reference<IcebergPositionalDeleteData> deletes = reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);

	// add this positional delete map information to the iceberg delete_data_map information
	delete_map->AddDeleteFileNameToDeleteDataEntry(initial_key, delete_file_name);
	delete_map->AddPosDeleteInfo(delete_file_name, initial_key);
	// delete_map->AddDataFileToPositionalDeleteFile(initial_key, delete_file_name);

	// initialize a set of data files and add the initial_key/current_file_paths
	// to the set.

	// on return add the set to teh

	for (idx_t i = 0; i < count; i++) {
		auto &name = names[i];
		auto &row_id = row_ids[i];

		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			// update our delete_map with the extra information
			auto new_data_file = name.GetString();
			delete_map->AddDeleteFileNameToDeleteDataEntry(new_data_file, delete_file_name);
			delete_map->AddPosDeleteInfo(delete_file_name, new_data_file);

			auto it = positional_delete_data.find(key);
			if (it == positional_delete_data.end()) {
				it =
				    positional_delete_data.emplace(key, make_uniq<IcebergPositionalDeleteData>(name.GetString())).first;
			}
			deletes = reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);
		}
		deletes.get().AddRow(row_id);
	}
}

} // namespace duckdb
