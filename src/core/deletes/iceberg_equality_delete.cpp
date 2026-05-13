#include "core/deletes/iceberg_equality_delete.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

static void InitializeFromOtherChunk(DataChunk &target, DataChunk &other, const vector<column_t> &column_ids) {
	vector<LogicalType> types;
	for (auto &id : column_ids) {
		types.push_back(other.data[id].GetType());
	}
	target.InitializeEmpty(types);
}

static void ColumnsReferencedByEqualityIds(DataChunk &source, DataChunk &result,
                                           const vector<MultiFileColumnDefinition> &local_columns,
                                           const vector<int32_t> &equality_ids) {
	//! Map from column_id to 'local_columns' index, to figure out which columns from the 'source' are relevant here
	unordered_map<int32_t, column_t> id_to_column;
	for (column_t i = 0; i < local_columns.size(); i++) {
		auto &col = local_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_column[col.identifier.GetValue<int32_t>()] = i;
	}

	vector<column_t> column_ids;
	for (auto id : equality_ids) {
		D_ASSERT(id_to_column.count(id));
		column_ids.push_back(id_to_column[id]);
	}
	//! Take only the relevant columns from the source
	InitializeFromOtherChunk(result, source, column_ids);
	result.ReferenceColumns(source, column_ids);
}

void IcebergMultiFileList::ScanEqualityDeleteFile(const BoundIcebergManifestEntry &bound_manifest_entry,
                                                  DataChunk &source, vector<MultiFileColumnDefinition> &local_columns,
                                                  const vector<MultiFileColumnDefinition> &global_columns,
                                                  const vector<ColumnIndex> &column_indexes) const {
	auto &manifest_entry = bound_manifest_entry.entry;
	auto &data_file = manifest_entry.data_file;
	auto &manifest_file = GetManifestFileForEntry(bound_manifest_entry, IcebergManifestContentType::DELETE);
	D_ASSERT(!data_file.equality_ids.empty());
	D_ASSERT(source.ColumnCount() == local_columns.size());

	auto count = source.size();
	if (count == 0) {
		return;
	}

	DataChunk result;
	ColumnsReferencedByEqualityIds(source, result, local_columns, data_file.equality_ids);

	const auto sequence_number = manifest_entry.GetSequenceNumber(manifest_file);
	//! Get or create the equality delete data for this sequence number
	auto it = equality_delete_data.find(sequence_number);
	if (it == equality_delete_data.end()) {
		it = equality_delete_data.emplace(sequence_number, make_uniq<IcebergEqualityDeleteData>(sequence_number)).first;
	}
	auto &deletes = *it->second;

	//! Map from column_id to 'global_columns' index, so we can resolve field-ids to schema positions.
	unordered_map<int32_t, column_t> id_to_global_column;
	for (column_t i = 0; i < global_columns.size(); i++) {
		auto &col = global_columns[i];
		D_ASSERT(!col.identifier.IsNull());
		id_to_global_column[col.identifier.GetValue<int32_t>()] = i;
	}

	deletes.files.emplace_back(data_file.partition_info, manifest_file.partition_spec_id);
	auto &rows = deletes.files.back().rows;
	rows.resize(count);
	D_ASSERT(result.ColumnCount() == data_file.equality_ids.size());
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto &field_id = data_file.equality_ids[col_idx];
		auto global_column_id = id_to_global_column[field_id];
		auto &col = global_columns[global_column_id];
		auto &vec = result.data[col_idx];

		// The iceberg pre-optimizer wraps every iceberg_scan in a LogicalFilter that
		// references every equality-delete column, so the column is guaranteed to live
		// in `column_indexes`. ApplyEqualityDeletes runs the predicate against
		// `input_chunk`, which is the file reader's local chunk — it contains only the
		// non-virtual entries of `column_indexes` in their original order. So the
		// BoundReferenceExpression index is the PHYSICAL position (column_indexes
		// position minus the number of preceding virtual columns).
		ColumnIndex equality_index(global_column_id);
		idx_t result_column_id = DConstants::INVALID_INDEX;
		idx_t physical_pos = 0;
		for (idx_t i = 0; i < column_indexes.size(); i++) {
			if (column_indexes[i].IsVirtualColumn()) {
				continue;
			}
			if (column_indexes[i] == equality_index) {
				result_column_id = physical_pos;
				break;
			}
			physical_pos++;
		}
		if (result_column_id == DConstants::INVALID_INDEX) {
			throw InternalException(
			    "ScanEqualityDeleteFile: required equality-delete column (field_id=%d) is missing from "
			    "the scan projection. The iceberg pre-optimizer should have ensured it was projected.",
			    field_id);
		}

		for (idx_t i = 0; i < count; i++) {
			auto &row = rows[i];
			auto constant = vec.GetValue(i);
			unique_ptr<Expression> equality_filter;
			auto bound_ref = make_uniq<BoundReferenceExpression>(col.type, result_column_id);
			if (!constant.IsNull()) {
				//! Create a COMPARE_NOT_EQUAL expression
				equality_filter =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(bound_ref),
				                                         make_uniq<BoundConstantExpression>(constant));
			} else {
				//! Construct an OPERATOR_IS_NOT_NULL expression instead
				auto is_not_null =
				    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
				is_not_null->children.push_back(std::move(bound_ref));
				equality_filter = std::move(is_not_null);
			}
			row.filters.emplace(std::make_pair(field_id, std::move(equality_filter)));
		}
	}
}

} // namespace duckdb
