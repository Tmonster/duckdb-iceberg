#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "common/iceberg_utils.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "function/iceberg_functions.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"

#include <string>
#include <numeric>

namespace duckdb {

static void AddNamedParameters(TableFunction &fun) {
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["mode"] = LogicalType::VARCHAR;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["version_name_format"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
	// Internal: set on the data-side iceberg_scan inside the bind_replace anti-join plan to suppress
	// the per-chunk equality-delete pass (the join above takes over that work).
	fun.named_parameters["__internal_skip_equality_deletes"] = LogicalType::BOOLEAN;
}

virtual_column_map_t IcebergVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto result = IcebergTableEntry::VirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

static void IcebergScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                 const TableFunction &function) {
	throw NotImplementedException("IcebergScan serialization not implemented");
}
static unique_ptr<FunctionData> IcebergScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("IcebergScan deserialization not implemented");
}

BindInfo IcebergBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<IcebergMultiFileList>();
	if (!file_list.table) {
		return BindInfo(ScanType::EXTERNAL);
	}
	return BindInfo(*file_list.table);
}

//! Build the children-list (positional + named-as-comparison) for a TableFunctionRef call to
//! iceberg_scan, mirroring what the user wrote, plus extra named parameters supplied by overrides.
static vector<unique_ptr<ParsedExpression>> BuildIcebergScanChildren(const vector<Value> &positional,
                                                                     const named_parameter_map_t &named_params,
                                                                     const named_parameter_map_t &extra_named) {
	vector<unique_ptr<ParsedExpression>> children;
	children.reserve(positional.size() + named_params.size() + extra_named.size());
	for (auto &v : positional) {
		children.push_back(make_uniq<ConstantExpression>(v));
	}
	auto add_named = [&](const string &key, const Value &value) {
		auto col_ref = make_uniq<ColumnRefExpression>(key);
		auto constant = make_uniq<ConstantExpression>(value);
		auto cmp =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(col_ref), std::move(constant));
		children.push_back(std::move(cmp));
	};
	for (auto &kv : named_params) {
		// Skip any user-supplied __internal flags; we re-set them below.
		if (StringUtil::CIEquals(kv.first, "__internal_skip_equality_deletes")) {
			continue;
		}
		add_named(kv.first, kv.second);
	}
	for (auto &kv : extra_named) {
		add_named(kv.first, kv.second);
	}
	return children;
}

//! Build a SubqueryRef wrapping `parquet_scan('<path>')` and projecting all of its columns plus a
//! constant `_iceberg_data_sequence_number` column. The subquery's alias is set so the join condition
//! can reference its columns.
static unique_ptr<SubqueryRef> BuildEqualityDeleteSubquery(const string &delete_file_path, int64_t sequence_number,
                                                           const string &alias) {
	// FROM parquet_scan('<delete_file_path>')
	vector<unique_ptr<ParsedExpression>> parquet_args;
	parquet_args.push_back(make_uniq<ConstantExpression>(Value(delete_file_path)));
	auto parquet_func = make_uniq<FunctionExpression>("parquet_scan", std::move(parquet_args));
	auto parquet_ref = make_uniq<TableFunctionRef>();
	parquet_ref->function = std::move(parquet_func);

	// SELECT *, CAST(<seq>::BIGINT AS BIGINT) AS _iceberg_data_sequence_number FROM (parquet_scan)
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto seq_constant = make_uniq<ConstantExpression>(Value::BIGINT(sequence_number));
	auto seq_cast = make_uniq<CastExpression>(LogicalType::BIGINT, std::move(seq_constant));
	seq_cast->alias = "_iceberg_data_sequence_number";
	select_node->select_list.push_back(std::move(seq_cast));
	select_node->from_table = std::move(parquet_ref);

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);

	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_stmt), alias);
	return subquery_ref;
}

//! Build the AND-conjunction condition for one anti-join: equality columns by name (NOT DISTINCT FROM)
//! plus `data._iceberg_data_sequence_number < delete._iceberg_data_sequence_number`.
static unique_ptr<ParsedExpression> BuildEqualityDeleteJoinCondition(const string &data_alias,
                                                                     const string &delete_alias,
                                                                     const vector<string> &equality_column_names) {
	vector<unique_ptr<ParsedExpression>> clauses;
	for (auto &col_name : equality_column_names) {
		auto left = make_uniq<ColumnRefExpression>(col_name, data_alias);
		auto right = make_uniq<ColumnRefExpression>(col_name, delete_alias);
		clauses.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(left),
		                                                  std::move(right)));
	}
	{
		auto data_seq = make_uniq<ColumnRefExpression>("_iceberg_data_sequence_number", data_alias);
		auto delete_seq = make_uniq<ColumnRefExpression>("_iceberg_data_sequence_number", delete_alias);
		clauses.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(data_seq),
		                                                  std::move(delete_seq)));
	}
	if (clauses.size() == 1) {
		return std::move(clauses[0]);
	}
	auto conjunction = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(clauses));
	return std::move(conjunction);
}

static unique_ptr<TableRef> IcebergScanBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	// Iceberg's `bind_replace` rewrites `iceberg_scan(...)` into a subtree of anti-joins between the
	// data scan and one synthesized scan per equality delete file. The recursive bind done by the
	// caller binds each piece as a normal table reference, so virtual columns (filename, _row_id,
	// etc.) are still accessible to the outer query — which would not be the case via bind_operator.

	// Re-entry guard: when our own anti-join plan refers to iceberg_scan with the skip flag, the
	// recursive binder reaches this hook again. Return nullptr so it falls through to the regular
	// bind path for that inner scan; otherwise we'd build the anti-join plan recursively forever.
	{
		auto it = input.named_parameters.find("__internal_skip_equality_deletes");
		if (it != input.named_parameters.end() && BooleanValue::Get(it->second)) {
			return nullptr;
		}
	}

	// 1. Pre-bind iceberg_scan once to discover the snapshot's equality delete files. We pass
	//    __internal_skip_equality_deletes=true so that on the eventual recursive bind the data side
	//    won't re-apply equality deletes via FinalizeChunk.
	auto data_named_params = input.named_parameters;
	data_named_params["__internal_skip_equality_deletes"] = Value::BOOLEAN(true);

	vector<LogicalType> probe_return_types;
	vector<string> probe_return_names;
	TableFunctionBindInput probe_bind_input(input.inputs, data_named_params, input.input_table_types,
	                                        input.input_table_names, input.info, input.binder, input.table_function,
	                                        input.ref);
	auto probe_bind_data = input.table_function.bind(context, probe_bind_input, probe_return_types, probe_return_names);

	auto &mfbd = probe_bind_data->Cast<MultiFileBindData>();
	auto &file_list = mfbd.file_list->Cast<IcebergMultiFileList>();

	{
		lock_guard<mutex> guard(file_list.delete_lock);
		file_list.EnumerateDeleteManifestEntries();
	}

	// Filter to equality-delete entries (parquet only — puffin is row-position deletes).
	struct EqualityDeleteFile {
		string file_path;
		int64_t sequence_number;
		vector<string> equality_column_names;
	};
	vector<EqualityDeleteFile> equality_files;
	auto &iceberg_schema = file_list.GetSchema().columns;
	unordered_map<int32_t, string> field_id_to_name;
	for (auto &col : iceberg_schema) {
		field_id_to_name[col->id] = col->name;
	}
	for (auto &entry : file_list.delete_manifest_entries) {
		auto &mft = entry.entry;
		if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
			continue;
		}
		if (!StringUtil::CIEquals(mft.data_file.file_format, "parquet")) {
			continue;
		}
		auto &manifest_file = file_list.GetManifestFileForEntry(entry, IcebergManifestContentType::DELETE);
		EqualityDeleteFile eq;
		eq.file_path = mft.data_file.file_path;
		if (file_list.options.allow_moved_paths) {
			auto iceberg_path = file_list.GetPath();
			auto &fs = FileSystem::GetFileSystem(context);
			eq.file_path = IcebergUtils::GetFullPath(iceberg_path, eq.file_path, fs);
		}
		eq.sequence_number = mft.GetSequenceNumber(manifest_file);
		for (auto field_id : mft.data_file.equality_ids) {
			auto it = field_id_to_name.find(field_id);
			if (it == field_id_to_name.end()) {
				throw InternalException("Equality delete references unknown field_id %d", field_id);
			}
			eq.equality_column_names.push_back(it->second);
		}
		equality_files.push_back(std::move(eq));
	}

	if (equality_files.empty()) {
		// No equality deletes — fall through to the regular bind path. Returning nullptr makes the
		// binder skip bind_replace, run bind, and treat iceberg_scan as a normal table function (with
		// virtual columns wired via AddTableFunction).
		return nullptr;
	}

	// 2. Build the data-side TableFunctionRef. iceberg_scan(<original positional args>, <named params>,
	//    __internal_skip_equality_deletes=true).
	const string data_alias = "__iceberg_data_scan";
	{
		named_parameter_map_t extra;
		extra["__internal_skip_equality_deletes"] = Value::BOOLEAN(true);
		auto data_children = BuildIcebergScanChildren(input.inputs, input.named_parameters, extra);
		auto data_func = make_uniq<FunctionExpression>("iceberg_scan", std::move(data_children));
		auto data_ref = make_uniq<TableFunctionRef>();
		data_ref->function = std::move(data_func);
		data_ref->alias = data_alias;

		unique_ptr<TableRef> root = std::move(data_ref);

		// 3. For each equality delete file, build a SubqueryRef around parquet_scan and chain an
		//    anti-join above the running root.
		for (idx_t i = 0; i < equality_files.size(); i++) {
			auto &eq = equality_files[i];
			string delete_alias = "__iceberg_eq_del_" + std::to_string(i);
			auto delete_ref = BuildEqualityDeleteSubquery(eq.file_path, eq.sequence_number, delete_alias);
			auto cond = BuildEqualityDeleteJoinCondition(data_alias, delete_alias, eq.equality_column_names);

			auto join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
			join_ref->type = JoinType::ANTI;
			join_ref->left = std::move(root);
			join_ref->right = std::move(delete_ref);
			join_ref->condition = std::move(cond);
			root = std::move(join_ref);
		}
		return root;
	}
}

//! FIXME: needs v1.5.1, causes a crash on v1.5.0
// static bool IcebergScanSupportsPushdownType(const FunctionData &bind_data_p, idx_t column_id) {
//	// Don't push down filters on the _row_id virtual column
//	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
//		return false;
//	}

//	// Default behavior for other columns
//	return true;
//}

TableFunctionSet IcebergFunctions::GetIcebergScanFunction(ExtensionLoader &loader) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read

	auto &parquet_scan = loader.GetTableFunction("parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergMultiFileReader::CreateInstance;
		function.late_materialization = false;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = IcebergScanSerialize;
		function.deserialize = IcebergScanDeserialize;

		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = IcebergBindInfo;
		function.get_virtual_columns = IcebergVirtualColumns;
		function.get_partition_stats = IcebergMultiFileReader::IcebergGetPartitionStats;
		// function.supports_pushdown_type = IcebergScanSupportsPushdownType;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		AddNamedParameters(function);

		function.bind_replace = IcebergScanBindReplace;

		function.name = "iceberg_scan";
	}

	parquet_scan_copy.name = "iceberg_scan";
	return parquet_scan_copy;
}

} // namespace duckdb
