#include "catalog/glue/hive/iceberg_glue_hive_table.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "catalog/glue/hive/glue_types.hpp"
#include "catalog/glue/hive/hive_multi_file_reader.hpp"
#include "catalog/glue/iceberg_glue_catalog.hpp"
#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

IcebergGlueHiveTable::IcebergGlueHiveTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           GlueTableInfo table_info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)), columns(info.columns.Copy()) {
	this->internal = false;
	tags["table_type"] = GlueTableFormatToString(table_info.GetFormat());
}

unique_ptr<IcebergGlueHiveTable> IcebergGlueHiveTable::TryCreate(Catalog &catalog, SchemaCatalogEntry &schema,
                                                                 GlueTableInfo table_info) {
	if (table_info.IsView() || table_info.GetFormat() != GlueTableFormat::HIVE) {
		return nullptr;
	}
	CreateTableInfo info(schema, Identifier(table_info.name));
	for (auto &column : table_info.columns) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	// Hive tables store their partition columns separately, they are regular (trailing) columns for a scan
	for (auto &column : table_info.partition_keys) {
		info.columns.AddColumn(ColumnDefinition(Identifier(column.name), GlueTypes::ToLogicalType(column.type)));
	}
	return make_uniq<IcebergGlueHiveTable>(catalog, schema, info, std::move(table_info));
}

optional_ptr<IcebergGlueHiveTable> IcebergGlueHiveTable::TryGet(TableCatalogEntry &table) {
	return dynamic_cast<IcebergGlueHiveTable *>(&table);
}

optional_ptr<const IcebergGlueHiveTable> IcebergGlueHiveTable::TryGet(const TableCatalogEntry &table) {
	return dynamic_cast<const IcebergGlueHiveTable *>(&table);
}

const ColumnList &IcebergGlueHiveTable::GetColumns() const {
	return columns;
}

unique_ptr<BaseStatistics> IcebergGlueHiveTable::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo IcebergGlueHiveTable::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

virtual_column_map_t IcebergGlueHiveTable::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	return result;
}

vector<column_t> IcebergGlueHiveTable::GetRowIdColumns() const {
	return vector<column_t>();
}

void IcebergGlueHiveTable::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                                 LogicalUpdate &update, ClientContext &context) {
	throw NotImplementedException("Cannot UPDATE %s", DescribeForError());
}

string IcebergGlueHiveTable::DescribeForError() const {
	return StringUtil::Format("a Hive table (\"%s\" in Glue database \"%s\")", name.GetIdentifierName(),
	                          schema.name.GetIdentifierName());
}

GlueTableInfo IcebergGlueHiveTable::RefreshTableInfo(ClientContext &context) const {
	auto &glue_catalog = catalog.Cast<IcebergGlueCatalog>();
	IcebergMetastoreIdentifier identifier;
	identifier.namespace_items.push_back(table_info.database_name);
	identifier.name = table_info.name;
	auto response = glue_catalog.GetTable(context, identifier);
	if (!response) {
		throw CatalogException("Glue table '%s.%s' no longer exists", table_info.database_name, table_info.name);
	}
	return IcebergGlueCatalog::ParseTableInfo(response->GetRoot().GetMember("Table"));
}

TableFunction IcebergGlueHiveTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// Ask Glue for the table right before scanning: it may have been replaced by a table of another format
	auto latest_info = RefreshTableInfo(context);
	if (latest_info.GetFormat() != GlueTableFormat::HIVE) {
		throw NotImplementedException("Scan from table with type %s", latest_info.GetFormatName());
	}
	// The scan produces the columns this entry was planned with: data columns first, partition keys last. The SerDe
	// decides the file format (throws for unsupported SerDes).
	auto scan_info = make_shared_ptr<HiveScanInfo>();
	scan_info->catalog_name = catalog.GetName().GetIdentifierName();
	scan_info->database_name = latest_info.database_name;
	scan_info->table_name = latest_info.name;
	scan_info->table = this;
	scan_info->root_location = latest_info.location;
	scan_info->file_format = latest_info.GetFileFormat();
	scan_info->delimiter = latest_info.GetFieldDelimiter();
	scan_info->quote = latest_info.GetQuoteCharacter();
	scan_info->escape = latest_info.GetEscapeCharacter();
	scan_info->header = latest_info.HasHeader();
	if (IsTextFileFormat(scan_info->file_format)) {
		scan_info->compression = latest_info.GetTextCompression();
	}
	for (auto &column : GetColumns().Logical()) {
		scan_info->names.push_back(column.Name());
		scan_info->types.push_back(column.Type());
	}
	for (auto &key : table_info.partition_keys) {
		scan_info->partition_keys.push_back(key.name);
	}
	// the partitions as registered in Glue, each with its own location
	if (!scan_info->partition_keys.empty()) {
		auto &glue_catalog = catalog.Cast<IcebergGlueCatalog>();
		scan_info->partitions = glue_catalog.GetPartitions(context, latest_info.database_name, latest_info.name);
	}
	return BindHiveScan(context, std::move(scan_info), bind_data);
}

} // namespace duckdb
