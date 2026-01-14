#include "storage/iceberg_insert.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/iceberg_table_information.hpp"
#include "metadata/iceberg_column_definition.hpp"
#include "iceberg_multi_file_list.hpp"
#include "storage/irc_transaction.hpp"
#include "utils/iceberg_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                             physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                             unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

IcebergInsert::IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(&table), schema(nullptr) {
}

IcebergCopyOptions::IcebergCopyOptions(unique_ptr<CopyInfo> info_p, CopyFunction copy_function_p)
    : info(std::move(info_p)), copy_function(std::move(copy_function_p)) {
}

IcebergCopyInput::IcebergCopyInput(ClientContext &context, ICTableEntry &table)
    : catalog(table.catalog.Cast<IRCatalog>()), table(table), columns(table.GetColumns()),
      data_path(table.table_info.table_metadata.GetLocation()) {
	partition_data = nullptr;
}

// IcebergCopyInput::IcebergCopyInput(ClientContext &context, IRCSchemaEntry &schema, const ColumnList &columns,
//                                    const string &data_path_p)
//     : catalog(schema.catalog.Cast<IRCatalog>()), columns(columns) {
// }

unique_ptr<GlobalSinkState> IcebergInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<IcebergInsertGlobalState>();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static string ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	pos++;
	for (; pos < input.size(); pos++) {
		if (input[pos] == '"') {
			pos++;
			// check if this is an escaped quote
			if (pos < input.size() && input[pos] == '"') {
				// escaped quote
				result += '"';
				continue;
			}
			return result;
		}
		result += input[pos];
	}
	throw InvalidInputException("Failed to parse quoted value - unterminated quote");
}

static vector<string> ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

struct IcebergColumnStats {
	explicit IcebergColumnStats() = default;

	string min;
	string max;
	idx_t null_count = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;
};

static IcebergColumnStats ParseColumnStats(const vector<Value> col_stats) {
	IcebergColumnStats column_stats;
	for (idx_t stats_idx = 0; stats_idx < col_stats.size(); stats_idx++) {
		auto &stats_children = StructValue::GetChildren(col_stats[stats_idx]);
		auto &stats_name = StringValue::Get(stats_children[0]);
		auto &stats_value = StringValue::Get(stats_children[1]);
		if (stats_name == "min") {
			D_ASSERT(!column_stats.has_min);
			column_stats.min = stats_value;
			column_stats.has_min = true;
		} else if (stats_name == "max") {
			D_ASSERT(!column_stats.has_max);
			column_stats.max = stats_value;
			column_stats.has_max = true;
		} else if (stats_name == "null_count") {
			D_ASSERT(!column_stats.has_null_count);
			column_stats.has_null_count = true;
			column_stats.null_count = StringUtil::ToUnsigned(stats_value);
		} else if (stats_name == "column_size_bytes") {
			column_stats.column_size_bytes = StringUtil::ToUnsigned(stats_value);
		} else if (stats_name == "has_nan") {
			column_stats.has_contains_nan = true;
			column_stats.contains_nan = stats_value == "true";
		} else {
			throw NotImplementedException("Unsupported stats type \"%s\" in IcebergInsert::Sink()", stats_name);
		}
	}
	return column_stats;
}

static void AddToColDefMap(case_insensitive_map_t<optional_ptr<IcebergColumnDefinition>> &name_to_coldef,
                           string col_name_prefix, optional_ptr<IcebergColumnDefinition> column_def) {
	string column_name = column_def->name;
	if (!col_name_prefix.empty()) {
		column_name = col_name_prefix + "." + column_def->name;
	}
	if (column_def->IsIcebergPrimitiveType()) {
		name_to_coldef.emplace(column_name, column_def.get());
	} else {
		for (auto &child : column_def->children) {
			AddToColDefMap(name_to_coldef, column_name, child.get());
		}
	}
}

static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
                            optional_ptr<TableCatalogEntry> table) {
	D_ASSERT(table);
	// grab lock for written files vector
	lock_guard<mutex> guard(global_state.lock);
	auto &ic_table = table->Cast<ICTableEntry>();
	auto partition_id = ic_table.table_info.table_metadata.default_spec_id;
	for (idx_t r = 0; r < chunk.size(); r++) {
		IcebergManifestEntry data_file;
		data_file.file_path = chunk.GetValue(0, r).GetValue<string>();
		data_file.record_count = static_cast<int64_t>(chunk.GetValue(1, r).GetValue<idx_t>());
		data_file.file_size_in_bytes = static_cast<int64_t>(chunk.GetValue(2, r).GetValue<idx_t>());
		data_file.content = IcebergManifestEntryContentType::DATA;
		data_file.status = IcebergManifestEntryStatusType::ADDED;
		data_file.file_format = "parquet";

		if (partition_id) {
			data_file.partition_spec_id = static_cast<int32_t>(partition_id);
		} else {
			data_file.partition_spec_id = 0;
		}

		// extract the column stats
		auto column_stats = chunk.GetValue(4, r);
		auto &map_children = MapValue::GetChildren(column_stats);

		global_state.insert_count += data_file.record_count;

		auto table_current_schema_id = ic_table.table_info.table_metadata.current_schema_id;
		auto ic_schema = ic_table.table_info.table_metadata.schemas[table_current_schema_id];

		case_insensitive_map_t<optional_ptr<IcebergColumnDefinition>> column_info;
		for (auto &column : ic_schema->columns) {
			AddToColDefMap(column_info, "", column.get());
		}

		for (idx_t col_idx = 0; col_idx < map_children.size(); col_idx++) {
			auto &struct_children = StructValue::GetChildren(map_children[col_idx]);
			auto &col_name = StringValue::Get(struct_children[0]);
			auto &col_stats = MapValue::GetChildren(struct_children[1]);
			auto column_names = ParseQuotedList(col_name, '.');
			auto stats = ParseColumnStats(col_stats);
			auto normalized_col_name = StringUtil::Join(column_names, ".");

			auto ic_column_info = column_info.find(normalized_col_name);
			D_ASSERT(ic_column_info != column_info.end());
			if (ic_column_info->second->required && stats.has_null_count && stats.null_count > 0) {
				throw ConstraintException("NOT NULL constraint failed: %s.%s", table->name, normalized_col_name);
			}

			//! TODO: convert 'stats' into 'data_file.lower_bounds', upper_bounds, value_counts, null_value_counts,
			//! nan_value_counts ...
		}

		//! TODO: extract the partition info
		// auto partition_info = chunk.GetValue(5, r);
		// if (!partition_info.IsNull()) {
		//	auto &partition_children = MapValue::GetChildren(partition_info);
		//	for (idx_t col_idx = 0; col_idx < partition_children.size(); col_idx++) {
		//		auto &struct_children = StructValue::GetChildren(partition_children[col_idx]);
		//		auto &part_value = StringValue::Get(struct_children[1]);

		//		IcebergPartition file_partition_info;
		//		file_partition_info.partition_column_idx = col_idx;
		//		file_partition_info.partition_value = part_value;
		//		data_file.partition_values.push_back(std::move(file_partition_info));
		//	}
		//}

		global_state.written_files.push_back(std::move(data_file));
	}
}

SinkResultType IcebergInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	// TODO: pass through the partition id?
	AddWrittenFiles(global_state, chunk, table);

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType IcebergInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<IcebergInsertGlobalState>();
	auto value = Value::BIGINT(global_state.insert_count);
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType IcebergInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<IcebergInsertGlobalState>();

	auto &irc_table = table->Cast<ICTableEntry>();
	auto &table_info = irc_table.table_info;
	auto &transaction = IRCTransaction::Get(context, table->catalog);
	auto &irc_transaction = transaction.Cast<IRCTransaction>();

	lock_guard<mutex> guard(global_state.lock);
	if (!global_state.written_files.empty()) {
		if (table_info.IsTransactionLocalTable(irc_transaction)) {
			table_info.AddSnapshot(transaction, std::move(global_state.written_files));
		} else {
			// add the table to updated tables for the transaction.
			irc_transaction.updated_tables.emplace(table_info.GetTableKey(), table_info.Copy());
			auto &updated_table = irc_transaction.updated_tables.at(table_info.GetTableKey());
			updated_table.InitSchemaVersions();
			updated_table.AddSnapshot(transaction, std::move(global_state.written_files));
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string IcebergInsert::GetName() const {
	D_ASSERT(table);
	return "ICEBERG_INSERT";
}

InsertionOrderPreservingMap<string> IcebergInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
static optional_ptr<CopyFunctionCatalogEntry> TryGetCopyFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, name)->Cast<CopyFunctionCatalogEntry>();
}

static Value GetFieldIdValue(const IcebergColumnDefinition &column) {
	auto column_value = Value::BIGINT(column.id);
	if (column.children.empty()) {
		// primitive type - return the field-id directly
		return column_value;
	}
	// nested type - generate a struct and recurse into children
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(column_value));
	for (auto &child : column.children) {
		values.emplace_back(child->name, GetFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

static Value WrittenFieldIds(const IcebergTableSchema &schema) {
	auto &columns = schema.columns;

	child_list_t<Value> values;
	for (idx_t c_idx = 0; c_idx < columns.size(); c_idx++) {
		auto &column = columns[c_idx];
		values.emplace_back(column->name, GetFieldIdValue(*column));
	}
	return Value::STRUCT(std::move(values));
}

// unique_ptr<CopyInfo> GetBindInput(IcebergCopyInput &input) {
// 	// Create Copy Info
// 	auto info = make_uniq<CopyInfo>();
// 	info->file_path = input.data_path;
// 	info->format = "parquet";
// 	info->is_from = false;
// 	for (auto &option : input.options) {
// 		info->options[option.first] = option.second;
// 	}
// 	return info;
// }

vector<IcebergManifestEntry> IcebergInsert::GetInsertManifestEntries(IcebergInsertGlobalState &global_state) {
	lock_guard<mutex> guard(global_state.lock);
	return std::move(global_state.written_files);
}

static unique_ptr<Expression> CreateColumnReference(IcebergCopyInput &copy_input, const LogicalType &type,
                                                    idx_t column_index) {
	// physical plan generation: generate a reference directly
	return make_uniq<BoundReferenceExpression>(type, column_index);
}

static unique_ptr<Expression> GetColumnReference(IcebergCopyInput &copy_input,
                                                 const IcebergPartitionSpecField &field_id) {
	optional_idx index;
	auto &column_field_id = IcebergInsert::GetTopLevelColumn(copy_input, field_id, index);
	// maybe I can just return the partitioned field_id?
	return CreateColumnReference(copy_input, column_field_id.Type(), index.GetIndex());
}

static unique_ptr<Expression> GetFunction(ClientContext &context, IcebergCopyInput &copy_input,
                                          const string &function_name, const IcebergPartitionSpecField &field_id) {
	vector<unique_ptr<Expression>> children;
	children.push_back(GetColumnReference(copy_input, field_id));

	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(DEFAULT_SCHEMA, function_name, std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

static unique_ptr<Expression> GetPartitionExpression(ClientContext &context, IcebergCopyInput &copy_input,
                                                     const IcebergPartitionSpecField &field) {
	switch (field.transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return GetColumnReference(copy_input, field);
	case IcebergTransformType::YEAR:
		return GetFunction(context, copy_input, "year", field);
	case IcebergTransformType::MONTH:
		return GetFunction(context, copy_input, "month", field);
	case IcebergTransformType::DAY:
		return GetFunction(context, copy_input, "day", field);
	case IcebergTransformType::HOUR:
		return GetFunction(context, copy_input, "hour", field);
	default:
		throw NotImplementedException("Unsupported partition transform type in GetPartitionExpression");
	}
}

static string GetPartitionExpressionName(IcebergCopyInput &copy_input, const IcebergPartitionSpecField &field,
                                         case_insensitive_set_t &names) {
	string prefix;
	switch (field.transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return field.name;
	case IcebergTransformType::YEAR:
		prefix = "year";
		break;
	case IcebergTransformType::MONTH:
		prefix = "month";
		break;
	case IcebergTransformType::DAY:
		prefix = "day";
		break;
	case IcebergTransformType::HOUR:
		prefix = "hour";
		break;
	default:
		throw NotImplementedException("Unsupported partition transform type in GetPartitionExpressionName");
	}
	if (names.find(prefix) == names.end()) {
		// prefer only the transform (e.g. year)
		return prefix;
	}
	return prefix + "_" + field.name;
}

static void GeneratePartitionExpressions(ClientContext &context, IcebergCopyInput &copy_input,
                                         IcebergCopyOptions &copy_options) {
	bool all_identity = true;
	for (auto &field : copy_input.partition_data->fields) {
		if (field.transform.Type() != IcebergTransformType::IDENTITY) {
			all_identity = false;
			break;
		}
	}
	if (all_identity) {
		// all transforms are identity transforms - we can partition on the columns directly
		// just set up the correct references to the partition columns
		for (auto &field : copy_input.partition_data->fields) {
			optional_idx col_idx;
			// TODO: partition field id or source id?
			// TODO: What is happening with all this top level stuff?
			// IcebergInsert::GetTopLevelColumn(copy_input, field.partition_field_id, col_idx);
			copy_options.partition_columns.push_back(col_idx.GetIndex());
		}
		return;
	}
	idx_t virtual_column_count;
	// switch (copy_input.virtual_columns) {
	// case InsertVirtualColumns::WRITE_ROW_ID:
	// case InsertVirtualColumns::WRITE_SNAPSHOT_ID:
	// 	virtual_column_count = 1;
	// 	break;
	// case InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID:
	// 	virtual_column_count = 2;
	// 	break;
	// default:
	// 	virtual_column_count = 0;
	// 	break;
	// }
	// if we have partition columns that are NOT identity, we need to compute them separately, and NOT write them
	idx_t partition_column_start = copy_input.columns.PhysicalColumnCount() + virtual_column_count;
	for (idx_t part_idx = 0; part_idx < copy_input.partition_data->fields.size(); part_idx++) {
		copy_options.partition_columns.push_back(partition_column_start++);
	}
	copy_options.write_partition_columns = false;

	idx_t col_idx = 0;
	for (auto &col : copy_input.columns.Physical()) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, col.Type(), col_idx++));
	}
	// push any projected virtual columns
	for (idx_t i = 0; i < virtual_column_count; i++) {
		copy_options.projection_list.push_back(CreateColumnReference(copy_input, LogicalType::BIGINT, col_idx++));
	}
	// push the partition expressions
	case_insensitive_set_t names;
	for (auto &field : copy_input.partition_data->fields) {
		auto expr = GetPartitionExpression(context, copy_input, field);
		copy_options.names.push_back(GetPartitionExpressionName(copy_input, field, names));
		names.insert(copy_options.names.back());
		copy_options.expected_types.push_back(expr->return_type);
		copy_options.projection_list.push_back(std::move(expr));
	}
}

IcebergCopyOptions IcebergInsert::GetCopyOptions(ClientContext &context, IcebergCopyInput &copy_input) {
	auto info = make_uniq<CopyInfo>();
	auto &catalog = copy_input.catalog;
	info->file_path = copy_input.data_path;
	info->format = "parquet";
	info->is_from = false;
	D_ASSERT(!copy_input.field_data.empty());
	info->options["field_ids"] = std::move(copy_input.field_data);

	// string parquet_compression;
	// if (catalog.TryGetConfigOption("parquet_compression", parquet_compression, schema_id, table_id)) {
	// 	info->options["compression"].emplace_back(parquet_compression);
	// }
	// string parquet_version;
	// if (catalog.TryGetConfigOption("parquet_version", parquet_version, schema_id, table_id)) {
	// 	info->options["parquet_version"].emplace_back(parquet_version);
	// }
	// string parquet_compression_level;
	// if (catalog.TryGetConfigOption("parquet_compression_level", parquet_compression_level, schema_id, table_id)) {
	// 	info->options["compression_level"].emplace_back(parquet_compression_level);
	// }
	// string row_group_size;
	// if (catalog.TryGetConfigOption("parquet_row_group_size", row_group_size, schema_id, table_id)) {
	// 	info->options["row_group_size"].emplace_back(row_group_size);
	// }
	// string row_group_size_bytes;
	// if (catalog.TryGetConfigOption("parquet_row_group_size_bytes", row_group_size_bytes, schema_id, table_id)) {
	// 	info->options["row_group_size_bytes"].emplace_back(row_group_size_bytes + " bytes");
	// }
	// string per_thread_output_str;
	// bool per_thread_output = false;
	// if (catalog.TryGetConfigOption("per_thread_output", per_thread_output_str, schema_id, table_id)) {
	// 	per_thread_output = per_thread_output_str == "true";
	// }
	// idx_t target_file_size = catalog.GetConfigOption<idx_t>("target_file_size", schema_id, table_id,
	//                                                         DuckLakeCatalog::DEFAULT_TARGET_FILE_SIZE);

	// Always use native parquet geometry for writing
	// info->options["geoparquet_version"].emplace_back("NONE");

	// Get Parquet Copy function
	auto copy_fun = TryGetCopyFunction(*context.db, "parquet");
	if (!copy_fun) {
		throw MissingExtensionException("Did not find parquet copy function required to write to iceberg table");
	}

	// auto &fs = FileSystem::GetFileSystem(context);
	// if (!fs.IsRemoteFile(copy_input.data_path)) {
	// 	// create data path if it does not yet exist
	// 	try {
	// 		fs.CreateDirectoriesRecursive(copy_input.data_path);
	// 	} catch (...) {
	// 	}
	// }

	// Bind Copy Function
	CopyFunctionBindInput bind_input(*info);

	auto names_to_write = copy_input.columns.GetColumnNames();
	auto types_to_write = copy_input.columns.GetColumnTypes();

	vector<LogicalType> casted_types;
	// for (const auto &type : types_to_write) {
	// 	if (DuckLakeTypes::RequiresCast(type)) {
	// 		casted_types.push_back(DuckLakeTypes::GetCastedType(type));
	// 	} else {
	// 		casted_types.push_back(type);
	// 	}
	// }

	auto function_data = copy_fun->function.copy_to_bind(context, bind_input, names_to_write, casted_types);

	IcebergCopyOptions result(std::move(info), copy_fun->function);
	result.bind_data = std::move(function_data);

	result.use_tmp_file = false;
	result.filename_pattern.SetFilenamePattern("{uuidv7}");
	if (copy_input.partition_data) {
		result.partition_output = true;
		result.write_empty_file = true;
		result.rotate = false;
	} else {
		result.partition_output = false;
		result.write_empty_file = false;
		// file_size_bytes is currently only supported for unpartitioned writes
		// result.file_size_bytes = target_file_size;
		result.rotate = true;
	}
	result.file_path = copy_input.data_path;
	// StripTrailingSeparator(fs, result.file_path);
	result.file_extension = "parquet";
	result.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	// result.per_thread_output = per_thread_output;
	result.write_partition_columns = true;
	result.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	result.names = names_to_write;
	result.expected_types = types_to_write;

	if (copy_input.partition_data) {
		// we are partitioning - generate partition expressions (if any)
		GeneratePartitionExpressions(context, copy_input, result);
	}
	return result;
}

PhysicalOperator &IcebergInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan) {
	auto copy_options = GetCopyOptions(context, copy_input);

	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto &physical_copy = planner
	                          .Make<PhysicalCopyToFile>(copy_return_types, std::move(copy_options.copy_function),
	                                                    std::move(copy_options.bind_data), 1)
	                          .Cast<PhysicalCopyToFile>();

	physical_copy.file_path = std::move(copy_options.file_path);
	physical_copy.use_tmp_file = copy_options.use_tmp_file;
	physical_copy.filename_pattern = std::move(copy_options.filename_pattern);
	physical_copy.file_extension = std::move(copy_options.file_extension);
	physical_copy.overwrite_mode = copy_options.overwrite_mode;
	physical_copy.per_thread_output = copy_options.per_thread_output;
	physical_copy.file_size_bytes = copy_options.file_size_bytes;
	physical_copy.rotate = copy_options.rotate;
	physical_copy.return_type = copy_options.return_type;

	physical_copy.partition_output = copy_options.partition_output;
	physical_copy.write_partition_columns = copy_options.write_partition_columns;
	physical_copy.write_empty_file = copy_options.write_empty_file;
	physical_copy.partition_columns = std::move(copy_options.partition_columns);
	physical_copy.names = std::move(copy_options.names);
	physical_copy.expected_types = std::move(copy_options.expected_types);
	physical_copy.parallel = true;
	// physical_copy.hive_file_pattern =
	// 	copy_input.catalog.UseHiveFilePattern(!is_encrypted, copy_input.schema_id, copy_input.table_id);
	if (plan) {
		physical_copy.children.push_back(*plan);
	}

	return physical_copy;

	// Get Parquet Copy function
	// auto copy_fun = TryGetCopyFunction(*context.db, "parquet");
	// if (!copy_fun) {
	// 	throw MissingExtensionException("Did not find parquet copy function required to write to iceberg table");
	// }
	//
	// auto names_to_write = copy_input.columns.GetColumnNames();
	// auto types_to_write = copy_input.columns.GetColumnTypes();
	//
	// auto wat = GetBindInput(copy_input);
	// auto bind_input = CopyFunctionBindInput(*wat);
	//
	// auto function_data = copy_fun->function.copy_to_bind(context, bind_input, names_to_write, types_to_write);
	//
	// auto &physical_copy = planner.Make<PhysicalCopyToFile>(
	//     GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS), copy_fun->function,
	//     std::move(function_data), 1);
	// auto &physical_copy_ref = physical_copy.Cast<PhysicalCopyToFile>();
	//
	// vector<idx_t> partition_columns;
	//! TODO: support partitions
	// auto partitions = op.table.Cast<ICTableEntry>().snapshot->GetPartitionColumns();
	// if (partitions.size() != 0) {
	//	auto column_names = op.table.Cast<ICTableEntry>().GetColumns().GetColumnNames();
	//	for (int64_t i = 0; i < partitions.size(); i++) {
	//		for (int64_t j = 0; j < column_names.size(); j++) {
	//			if (column_names[j] == partitions[i]) {
	//				partition_columns.push_back(j);
	//				break;
	//			}
	//		}
	//	}
	//}

	// physical_copy_ref.use_tmp_file = false;
	// if (!partition_columns.empty()) {
	// 	physical_copy_ref.filename_pattern.SetFilenamePattern("{uuidv7}");
	// 	physical_copy_ref.file_path = copy_input.data_path;
	// 	physical_copy_ref.partition_output = true;
	// 	physical_copy_ref.partition_columns = partition_columns;
	// 	physical_copy_ref.write_empty_file = true;
	// 	physical_copy_ref.rotate = false;
	// } else {
	// 	physical_copy_ref.filename_pattern.SetFilenamePattern("{uuidv7}");
	// 	physical_copy_ref.file_path = copy_input.data_path;
	// 	physical_copy_ref.partition_output = false;
	// 	physical_copy_ref.write_empty_file = false;
	// 	physical_copy_ref.file_size_bytes = IRCatalog::DEFAULT_TARGET_FILE_SIZE;
	// 	physical_copy_ref.rotate = true;
	// }
	//
	// physical_copy_ref.file_extension = "parquet";
	// physical_copy_ref.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	// physical_copy_ref.per_thread_output = false;
	// physical_copy_ref.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS; // TODO: capture stats
	// physical_copy_ref.write_partition_columns = true;
	// if (plan) {
	// 	physical_copy.children.push_back(*plan);
	// }
	// physical_copy_ref.names = names_to_write;
	// physical_copy_ref.expected_types = types_to_write;
	// physical_copy_ref.hive_file_pattern = true;
	// return physical_copy;
}

void VerifyDirectInsertionOrder(LogicalInsert &op) {
	idx_t column_index = 0;
	for (auto &mapping : op.column_index_map) {
		if (mapping == DConstants::INVALID_INDEX || mapping != column_index) {
			//! See issue#444
			throw NotImplementedException("Iceberg inserts don't support targeted inserts yet (i.e tbl(col1,col2))");
		}
		column_index++;
	}
}

PhysicalOperator &IcebergInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                            ICTableEntry &table) {
	optional_idx partition_id;
	vector<LogicalType> return_types;
	// the one return value is how many rows we are inserting
	return_types.emplace_back(LogicalType::BIGINT);
	return planner.Make<IcebergInsert>(return_types, table);
}

PhysicalOperator &IRCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                        optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Iceberg table");
	}

	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Iceberg table");
	}

	VerifyDirectInsertionOrder(op);

	auto &table_entry = op.table.Cast<ICTableEntry>();
	// FIXME: Inserts into V3 tables is not yet supported since
	// we need to keep track of row lineage, which we do not support
	// https://iceberg.apache.org/spec/#row-lineage
	if (table_entry.table_info.table_metadata.iceberg_version == 3) {
		throw NotImplementedException("Insert into Iceberg V3 tables");
	}
	table_entry.PrepareIcebergScanFromEntry(context);
	auto &table_info = table_entry.table_info;
	auto &schema = table_info.table_metadata.GetLatestSchema();

	auto &partition_spec = table_info.table_metadata.GetLatestPartitionSpec();
	if (table_info.table_metadata.HasSortOrder()) {
		auto &sort_spec = table_info.table_metadata.GetLatestSortOrder();
		if (sort_spec.IsSorted()) {
			throw NotImplementedException("INSERT into a sorted iceberg table is not supported yet");
		}
	}

	// I should be able to set this up in PlanCopyForInsert if I have the ICTable in the info.
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(schema));

	auto info = make_uniq<IcebergCopyInput>(context, table_entry);
	info->field_data = std::move(field_input);

	auto &insert = planner.Make<IcebergInsert>(op, op.table, op.column_index_map);

	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, *info, plan);

	insert.children.push_back(physical_copy);

	return insert;
}

PhysicalOperator &IRCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalCreateTable &op, PhysicalOperator &plan) {
	// TODO: parse partition information here.
	auto &schema = op.schema;

	auto &ic_schema_entry = schema.Cast<IRCSchemaEntry>();
	auto &catalog = ic_schema_entry.catalog;
	auto transaction = catalog.GetCatalogTransaction(context);

	// create the table. Takes care of committing to rest catalog and getting the metadata location etc.
	// setting the schema
	auto table = ic_schema_entry.CreateTable(transaction, context, *op.info);
	if (!table) {
		throw InternalException("Table could not be created");
	}
	auto &ic_table = table->Cast<ICTableEntry>();
	// We need to load table credentials into our secrets for when we copy files
	ic_table.PrepareIcebergScanFromEntry(context);

	auto &table_schema = ic_table.table_info.table_metadata.GetLatestSchema();

	// I should be able to set this up in PlanCopyForInsert if I have the ICTable in the info.
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(table_schema));
	// Create Copy Info
	auto info = make_uniq<IcebergCopyInput>(context, ic_table);
	info->field_data = std::move(field_input);

	auto &physical_copy = IcebergInsert::PlanCopyForInsert(context, planner, *info, plan);
	physical_index_vector_t<idx_t> column_index_map;
	auto &insert = planner.Make<IcebergInsert>(op, ic_table, column_index_map);

	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
