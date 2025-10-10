//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/iceberg_metadata_info.hpp"

namespace duckdb {

struct IcebergDeleteData {
	// the parquet file with the data
	string delete_file_name;
	// the rows deleted from that file after reading all the positional delete data
	unordered_set<int64_t> deleted_rows;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) const;
};

class IcebergDeleteFilter : public DeleteFilter {
public:
	IcebergDeleteFilter() {
		delete_data = make_shared_ptr<IcebergDeleteData>();
	};

protected:
	shared_ptr<IcebergDeleteData> delete_data;
	optional_idx max_row_count;

public:
	virtual idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) = 0;
	void Initialize(ClientContext &context, const IcebergFileData &delete_file);
	void SetMaxRowCount(idx_t max_row_count);
	shared_ptr<IcebergDeleteData> GetDeleteData() const {
		return delete_data;
	}

private:
	static vector<idx_t> ScanDeleteFile(ClientContext &context, const IcebergFileData &delete_file);
};

} // namespace duckdb
