#include "execution/operator/iceberg_delete.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
#include "common/iceberg_utils.hpp"
#include "core/expression/iceberg_value.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

//! Equality-delete write helpers. The functions defined here are only invoked when the
//! ICEBERG_ENABLE_EQUALITY_DELETE_WRITES compile flag is on - in default builds the callers
//! (in iceberg_delete.cpp) are #ifdef'd out, so this code is dead.

static bool ExpressionContainsFunction(const Expression &expr, const char *function_name) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
	    expr.Cast<BoundFunctionExpression>().function.name == function_name) {
		return true;
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!found && ExpressionContainsFunction(child, function_name)) {
			found = true;
		}
	});
	return found;
}

static bool IsColumnReference(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_REF;
}

static bool IsConstant(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
}

//! Whether a physical-filter expression is built purely from equality-delete forms, i.e. `col = const`,
//! `col IN (const, ...)`, and AND/OR of those. `col IN (...)` and `col = c1 OR ...` push down as an
//! optional scan filter but still leave this physical filter behind; recognizing it here lets that delete
//! stay on the equality-delete path. Anything else (ranges, functions, arbitrary expressions) disqualifies.
static bool ExpressionIsEqualityDeleteForm(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = expr.Cast<BoundComparisonExpression>();
		if (comparison.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		return (IsColumnReference(*comparison.left) && IsConstant(*comparison.right)) ||
		       (IsConstant(*comparison.left) && IsColumnReference(*comparison.right));
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.GetExpressionType() != ExpressionType::COMPARE_IN || op.children.size() < 2 ||
		    !IsColumnReference(*op.children[0])) {
			return false;
		}
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (!IsConstant(*op.children[i])) {
				return false;
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.children) {
			if (!ExpressionIsEqualityDeleteForm(*child)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

static bool PlanContainsPhysicalFilter(PhysicalOperator &plan) {
	if (plan.type == PhysicalOperatorType::FILTER) {
		auto &filter = plan.Cast<PhysicalFilter>();
		//! Two physical filters are compatible with writing an equality delete and must not disqualify it:
		//!  - the 'iceberg_verify_equality_deletes' filter the read path injects to apply existing equality
		//!    deletes (otherwise every delete after the first falls back to positional deletes), and
		//!  - the DELETE predicate itself when it is a pure equality form (`col IN (...)` / `col = c1 OR ...`
		//!    leave this filter behind even though they also push down as an optional scan filter).
		if (!ExpressionContainsFunction(*filter.expression, "iceberg_verify_equality_deletes") &&
		    !ExpressionIsEqualityDeleteForm(*filter.expression)) {
			return true;
		}
	}
	for (auto &child : plan.children) {
		if (PlanContainsPhysicalFilter(child.get())) {
			return true;
		}
	}
	return false;
}

//! Collect the constant values a single-column table filter deletes, for the equality-delete forms we
//! support: `col = c` (one value), `col IN (c1, ...)`, and `col = c1 OR col = c2 OR ...` (which pushes down
//! as an OR-conjunction of constant comparisons). Returns false for any other filter shape. NULL constants
//! are dropped - they can never match an equality, so they contribute no deleted rows.
static bool TryExtractEqualityDeleteValues(const TableFilter &filter, vector<Value> &values) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		if (constant_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		if (!constant_filter.constant.IsNull()) {
			values.push_back(constant_filter.constant);
		}
		return true;
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (auto &value : in_filter.values) {
			if (!value.IsNull()) {
				values.push_back(value);
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : conjunction_filter.child_filters) {
			//! Every disjunct must itself be a supported equality form on this same column.
			if (!TryExtractEqualityDeleteValues(*child, values)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		//! `col IN (...)` / `col = c1 OR ...` push down as an OPTIONAL_FILTER wrapping the real filter.
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			return false;
		}
		return TryExtractEqualityDeleteValues(*optional_filter.child_filter, values);
	}
	default:
		return false;
	}
}

bool IcebergDelete::TryGetEqualityDeletePredicates(ClientContext &context, IcebergTableEntry &table,
                                                   PhysicalOperator &child_plan,
                                                   vector<IcebergEqualityDeletePredicate> &equality_predicates) {
	//! Gated behind an explicit testing-only setting.
	Value setting_value;
	if (!context.TryGetCurrentSetting(ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE, setting_value) ||
	    setting_value.IsNull() || !setting_value.GetValue<bool>()) {
		return false;
	}

	//! Equality-delete writing is only supported for v2, unpartitioned tables.
	auto &table_metadata = table.table_info.table_metadata;
	if (table_metadata.iceberg_version != 2) {
		return false;
	}
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		return false;
	}

	//! Any filter means this cannot be an equality delete.
	if (PlanContainsPhysicalFilter(child_plan)) {
		return false;
	}

	auto table_scan = FindDeleteSource(child_plan);
	if (!table_scan) {
		return false;
	}
	auto &scan = *table_scan;
	if (!scan.table_filters || scan.table_filters->filters.empty()) {
		return false;
	}

	auto &schema = table_metadata.GetLatestSchema();
	auto &columns = schema.columns;
	for (auto &filter_entry : scan.table_filters->filters) {
		auto column_key = filter_entry.first;
		auto &table_filter = *filter_entry.second;
		//! Accept `col = c`, `col IN (...)`, or `col = c1 OR col = c2 OR ...`; reject anything else.
		vector<Value> raw_values;
		if (!TryExtractEqualityDeleteValues(table_filter, raw_values)) {
			return false;
		}
		if (raw_values.empty()) {
			//! e.g. `col IN (NULL)` - nothing to delete via equality; fall back to positional.
			return false;
		}
		if (column_key >= scan.column_ids.size()) {
			return false;
		}
		auto &column_index = scan.column_ids[column_key];
		if (column_index.IsVirtualColumn()) {
			return false;
		}
		auto primary_index = column_index.GetPrimaryIndex();
		if (primary_index >= columns.size()) {
			return false;
		}
		auto &column_definition = *columns[primary_index];
		//! The same column referenced more than once is not a clean equality delete.
		for (auto &existing : equality_predicates) {
			if (existing.field_id == column_definition.id) {
				return false;
			}
		}
		IcebergEqualityDeletePredicate predicate;
		predicate.field_id = column_definition.id;
		predicate.column_name = column_definition.name;
		predicate.type = column_definition.type;
		for (auto &raw_value : raw_values) {
			Value delete_value;
			string error_message;
			if (!raw_value.DefaultTryCastAs(column_definition.type, delete_value, &error_message, true)) {
				return false;
			}
			predicate.values.push_back(std::move(delete_value));
		}
		equality_predicates.push_back(std::move(predicate));
	}

	if (equality_predicates.empty()) {
		return false;
	}
	//! The equality-delete file materializes the cross product of every column's value set. Cap it so a
	//! very large delete falls back to positional deletes instead of writing a huge equality-delete file.
	static constexpr idx_t MAX_EQUALITY_DELETE_ROWS = 4096;
	idx_t total_rows = 1;
	for (auto &predicate : equality_predicates) {
		total_rows *= predicate.values.size();
		if (total_rows > MAX_EQUALITY_DELETE_ROWS) {
			return false;
		}
	}
	return true;
}

void IcebergDelete::WriteEqualityDeleteFile(ClientContext &context, IcebergDeleteGlobalState &global_state) const {
	D_ASSERT(!equality_predicates.empty());

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = table.table_info.table_metadata.GetDataPath(fs);
	string delete_filename = UUID::ToString(UUID::GenerateRandomUUID()) + "-equality-deletes.parquet";
	string delete_file_path = fs.JoinPath(data_path, delete_filename);

	auto info = make_uniq<CopyInfo>();
	info->file_path = delete_file_path;
	info->format = "parquet";
	info->is_from = false;

	// Generate the field ids for the parquet writer: every column carries, as PARQUET:field_id
	// metadata, the iceberg field-id that the equality delete applies to.
	child_list_t<Value> field_id_values;
	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	vector<int32_t> equality_ids;
	for (auto &predicate : equality_predicates) {
		field_id_values.emplace_back(predicate.column_name, Value::INTEGER(predicate.field_id));
		names_to_write.push_back(predicate.column_name);
		types_to_write.push_back(predicate.type);
		equality_ids.push_back(predicate.field_id);
	}
	vector<Value> field_input;
	field_input.push_back(Value::STRUCT(std::move(field_id_values)));
	info->options["field_ids"] = std::move(field_input);

	auto &copy_fun = IcebergUtils::GetCopyFunction(context, "parquet");
	CopyFunctionBindInput bind_input(*info);

	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_to_write, types_to_write);
	auto copy_global_state = copy_fun.function.copy_to_initialize_global(context, *function_data, delete_file_path);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto copy_local_state = copy_fun.function.copy_to_initialize_local(execution_context, *function_data);

	CopyFunctionFileStatistics stats;
	copy_fun.function.copy_to_get_written_statistics(context, *function_data, *copy_global_state, stats);

	// Materialize the equality-delete rows: the cross product of every column's value set. Within a row
	// the columns are AND-ed and rows are OR-ed, encoding `(col0 IN vals0) AND (col1 IN vals1) AND ...`.
	vector<vector<Value>> rows;
	rows.emplace_back();
	for (auto &predicate : equality_predicates) {
		vector<vector<Value>> expanded;
		for (auto &existing_row : rows) {
			for (auto &value : predicate.values) {
				auto new_row = existing_row;
				new_row.push_back(value);
				expanded.push_back(std::move(new_row));
			}
		}
		rows = std::move(expanded);
	}

	// Write the delete tuples (one per row), chunking at STANDARD_VECTOR_SIZE.
	idx_t rows_written = 0;
	while (rows_written < rows.size()) {
		idx_t chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows.size() - rows_written);
		DataChunk write_chunk;
		write_chunk.Initialize(context, types_to_write);
		for (idx_t row_idx = 0; row_idx < chunk_count; row_idx++) {
			auto &row = rows[rows_written + row_idx];
			for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
				write_chunk.data[col_idx].SetValue(row_idx, row[col_idx]);
			}
		}
		write_chunk.SetCardinality(chunk_count);
		copy_fun.function.copy_to_sink(execution_context, *function_data, *copy_global_state, *copy_local_state,
		                               write_chunk);
		rows_written += chunk_count;
	}

	copy_fun.function.copy_to_combine(execution_context, *function_data, *copy_global_state, *copy_local_state);
	copy_fun.function.copy_to_finalize(context, *function_data, *copy_global_state);

	IcebergDeleteFileInfo delete_file;
	delete_file.file_name = delete_file_path;
	delete_file.file_format = "parquet";
	delete_file.delete_count = rows.size();
	delete_file.file_size_bytes = stats.file_size_bytes;
	delete_file.equality_ids = std::move(equality_ids);

	// Record per-field metrics for the equality-delete values so that scans can prune this delete file
	// when its equality-field range is disjoint from the scan predicate / a data file's bounds. Each
	// field's bound spans the min/max of its value set.
	for (auto &predicate : equality_predicates) {
		Value min_value = predicate.values[0];
		Value max_value = predicate.values[0];
		for (auto &value : predicate.values) {
			if (value < min_value) {
				min_value = value;
			}
			if (value > max_value) {
				max_value = value;
			}
		}
		auto lower = IcebergValue::SerializeValue(min_value, predicate.type, SerializeBound::LOWER_BOUND);
		if (lower.HasError()) {
			throw InvalidConfigurationException(lower.GetError());
		} else if (lower.HasValue()) {
			delete_file.lower_bounds[predicate.field_id] = lower.GetValue();
		}
		auto upper = IcebergValue::SerializeValue(max_value, predicate.type, SerializeBound::UPPER_BOUND);
		if (upper.HasError()) {
			throw InvalidConfigurationException(upper.GetError());
		} else if (upper.HasValue()) {
			delete_file.upper_bounds[predicate.field_id] = upper.GetValue();
		}
	}

	global_state.written_files.emplace(delete_file_path, std::move(delete_file));
}

} // namespace duckdb
