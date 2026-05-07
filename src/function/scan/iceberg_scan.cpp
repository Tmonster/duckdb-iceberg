#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
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

//! Refuse filter pushdown on columns that are also referenced by an equality-delete file.
//!
//! Why: when filter pushdown converts `WHERE id = X` into a TableFilter on `id`, the column-pruning
//! optimizer drops `id` from `LogicalGet.projection_ids` (only the TableFilter references it now,
//! not the projection). The runtime equality-delete machinery in `IcebergMultiFileReader::Finalize-
//! Chunk` evaluates `(id != value) OR ...` on the projected `output_chunk`, where `id` is no
//! longer materialized — crash. By refusing pushdown for these columns we keep the predicate as a
//! `LogicalFilter` above the LogicalGet, which keeps `id` in `projection_ids` and therefore in
//! `output_chunk`. The trade-off is losing parquet-side row-group/page filtering for those filters,
//! which is fine for the typical case (low cardinality of equality-delete columns).
static bool IcebergScanSupportsPushdownType(const FunctionData &bind_data_p, idx_t column_id) {
	auto &bind_data = bind_data_p.Cast<MultiFileBindData>();
	if (!bind_data.file_list) {
		return true;
	}
	auto iceberg_list = dynamic_cast<IcebergMultiFileList *>(bind_data.file_list.get());
	if (!iceberg_list) {
		return true;
	}
	auto &equality_field_ids = iceberg_list->GetEqualityDeleteFieldIds();
	if (equality_field_ids.empty()) {
		return true;
	}
	// column_id is the position in the LogicalGet's `returned_types`/`returned_names`, which is
	// iceberg-schema order. Map it to a field_id and check membership.
	auto &iceberg_schema = iceberg_list->GetSchema().columns;
	if (column_id >= iceberg_schema.size()) {
		// Virtual columns (filename, _row_id, …) live above the regular schema; pushdown is fine.
		return true;
	}
	auto field_id = iceberg_schema[column_id]->id;
	return equality_field_ids.count(field_id) == 0;
}

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
		function.supports_pushdown_type = IcebergScanSupportsPushdownType;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		AddNamedParameters(function);

		function.name = "iceberg_scan";
	}

	parquet_scan_copy.name = "iceberg_scan";
	return parquet_scan_copy;
}

} // namespace duckdb
