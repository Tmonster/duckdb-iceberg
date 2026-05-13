#include "planning/iceberg_optimizer.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

static unique_ptr<LogicalOperator> WrapIcebergScans(ClientContext &context, unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = WrapIcebergScans(context, std::move(child));
	}
	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return op;
	}
	auto &get = op->Cast<LogicalGet>();
	if (get.function.name != "iceberg_scan" || !get.bind_data) {
		return op;
	}
	auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
	if (!mfbd.file_list) {
		return op;
	}
	auto iceberg_list = dynamic_cast<IcebergMultiFileList *>(mfbd.file_list.get());
	if (!iceberg_list) {
		return op;
	}

	{
		lock_guard<mutex> guard(iceberg_list->delete_lock);
		iceberg_list->EnumerateDeleteManifestEntries();
	}

	unordered_set<int32_t> required_field_ids;
	for (auto &entry : iceberg_list->delete_manifest_entries) {
		auto &mft = entry.entry;
		if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
			continue;
		}
		for (auto fid : mft.data_file.equality_ids) {
			required_field_ids.insert(fid);
		}
	}
	if (required_field_ids.empty()) {
		return op;
	}

	auto &schema_columns = iceberg_list->GetSchema().columns;
	vector<unique_ptr<Expression>> args;
	for (auto fid : required_field_ids) {
		idx_t schema_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < schema_columns.size(); i++) {
			if (schema_columns[i]->id == fid) {
				schema_idx = i;
				break;
			}
		}
		if (schema_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		idx_t local_idx = DConstants::INVALID_INDEX;
		const auto &col_ids = get.GetColumnIds();
		for (idx_t i = 0; i < col_ids.size(); i++) {
			if (!col_ids[i].IsVirtualColumn() && col_ids[i].GetPrimaryIndex() == schema_idx) {
				local_idx = i;
				break;
			}
		}
		if (local_idx == DConstants::INVALID_INDEX) {
			get.AddColumnId(schema_idx);
			local_idx = get.GetColumnIds().size() - 1;
		}
		auto bindings = get.GetColumnBindings();
		args.push_back(make_uniq<BoundColumnRefExpression>(schema_columns[schema_idx]->type, bindings[local_idx]));
	}
	if (args.empty()) {
		return op;
	}

	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &fn_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "verify_equality_delete_columns");
	FunctionBinder function_binder(context);
	vector<LogicalType> arg_types;
	for (auto &a : args) {
		arg_types.push_back(a->return_type);
	}
	auto fn = fn_entry.functions.GetFunctionByArguments(context, arg_types);
	auto bound_call = function_binder.BindScalarFunction(fn, std::move(args));

	auto filter = make_uniq<LogicalFilter>();
	filter->expressions.push_back(std::move(bound_call));
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

static void IcebergPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	plan = WrapIcebergScans(input.context, std::move(plan));
}

OptimizerExtension IcebergOptimizerExtension::Create() {
	OptimizerExtension ext;
	ext.pre_optimize_function = IcebergPreOptimize;
	return ext;
}

} // namespace duckdb
