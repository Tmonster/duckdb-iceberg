#include "planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include "function/iceberg_functions.hpp"
#include "planning/iceberg_multi_file_reader.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"

namespace duckdb {

static virtual_column_map_t IcebergDeleteVirtualColumns(ClientContext &context,
                                                        optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto result = IcebergTableEntry::VirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

static void IcebergDeletesScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                        const TableFunction &function) {
	throw NotImplementedException("IcebergDeletesScan serialization not implemented");
}
static unique_ptr<FunctionData> IcebergDeletesScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("IcebergDeletesScan deserialization not implemented");
}

TableFunctionSet IcebergFunctions::GetIcebergDeletesScanFunction(ClientContext &context) {
	// The iceberg_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// IcebergMultiFileReader into it to create a Iceberg-based multi file read
	auto &instance = DatabaseInstance::GetDatabase(context);
	//! FIXME: delete files could also be made without row_ids,
	//! in which case we need to rely on the `'schema.column-mapping.default'` property just like data files do.
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "parquet_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"parquet_scan\" not found!");
	}
	auto &parquet_scan = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = IcebergDeleteFileReader::CreateInstance;
		function.late_materialization = false;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = IcebergDeletesScanSerialize;
		function.deserialize = IcebergDeletesScanDeserialize;

		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = nullptr;
		function.get_virtual_columns = IcebergDeleteVirtualColumns;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");
		function.name = "iceberg_deletes_scan";
	}

	parquet_scan_copy.name = "iceberg_deletes_scan";
	return parquet_scan_copy;
}

IcebergDeleteFileReader::IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info)
    : function_info(function_info) {
}

unique_ptr<MultiFileReader> IcebergDeleteFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<IcebergDeleteFileReader>(table.function_info);
}

unique_ptr<Expression> IcebergDeleteFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    idx_t &column_id, const LogicalType &type, MultiFileLocalIndex local_idx,
    optional_ptr<MultiFileColumnDefinition> &global_column_reference) {
	if (column_id == IcebergMultiFileReader::COLUMN_IDENTIFIER_DATA_SEQUENCE_NUMBER) {
		// `file_to_be_opened` is only populated when the reader was constructed for an *unopened* file;
		// for the bind-time initial_reader (already open), look at `reader_data.reader->file` instead.
		shared_ptr<ExtendedOpenFileInfo> info;
		if (reader_data.file_to_be_opened.extended_info) {
			info = reader_data.file_to_be_opened.extended_info;
		} else if (reader_data.reader) {
			info = reader_data.reader->file.extended_info;
		}
		if (!info) {
			return make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT));
		}
		auto entry = info->options.find("sequence_number");
		if (entry == info->options.end()) {
			return make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT));
		}
		return make_uniq<BoundConstantExpression>(entry->second);
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type, local_idx,
	                                                   global_column_reference);
}

shared_ptr<MultiFileList> IcebergDeleteFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                  const FileGlobInput &glob_input) {
	D_ASSERT(paths.size() == 1);
	vector<OpenFileInfo> open_files;
	// in case someone calls this
	if (!function_info) {
		throw NotImplementedException("IcebergDeleteFileReader must be called with function info");
	}
	auto &iceberg_delete_function_info = function_info->Cast<IcebergDeleteScanInfo>();
	auto &extended_delete_info = iceberg_delete_function_info.file_info;
	open_files.emplace_back(extended_delete_info);
	auto res = make_uniq<SimpleMultiFileList>(std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
