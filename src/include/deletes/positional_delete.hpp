#pragma once

#include "storage/iceberg_delete_filter.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"

namespace duckdb {

class IcebergPositionalDeleteData : public IcebergDeleteFilter {
public:
	IcebergPositionalDeleteData(const string delete_file_path) : IcebergDeleteFilter() {
		delete_data->delete_file_name = delete_file_path;
	}

public:
	void AddRow(int64_t row_id) {
		delete_data->deleted_rows.insert(row_id);
	}

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override {
		if (count == 0) {
			return 0;
		}
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t selection_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (!delete_data->deleted_rows.count(i + start_row_index)) {
				result_sel.set_index(selection_idx++, i);
			}
		}
		return selection_idx;
	}
};

} // namespace duckdb
