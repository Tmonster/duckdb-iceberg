#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "catalog/glue/hive/glue_info.hpp"

namespace duckdb {

class IcebergGlueCatalog;

//! A Hive table of a Glue catalog: a Glue table without the Iceberg 'table_type', whose data files are read with the
//! reader its SerDe names (parquet, csv, json or avro) over the partitions Glue lists (see HiveMultiFileReader, ported
//! from the aws_glue extension).
class IcebergGlueHiveTable : public TableCatalogEntry {
public:
	IcebergGlueHiveTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, GlueTableInfo table_info);

public:
	//! The entry for a Glue table, or nullptr if it isn't a Hive table (Delta, Hudi, views, ...)
	static unique_ptr<IcebergGlueHiveTable> TryCreate(Catalog &catalog, SchemaCatalogEntry &schema,
	                                                  GlueTableInfo table_info);
	//! Whether 'table' is a Hive table of a Glue catalog
	static optional_ptr<IcebergGlueHiveTable> TryGet(TableCatalogEntry &table);
	static optional_ptr<const IcebergGlueHiveTable> TryGet(const TableCatalogEntry &table);

public:
	const ColumnList &GetColumns() const override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	//! filename: the location of the data file a row was read from, as for read_parquet
	virtual_column_map_t GetVirtualColumns() const override;
	//! None: the files of a Hive table have no row identifier
	vector<column_t> GetRowIdColumns() const override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

	//! "a Hive table ("t" in Glue database "db")", for the errors of statements Hive tables don't support
	string DescribeForError() const;
	//! Re-fetch the table definition from Glue
	GlueTableInfo RefreshTableInfo(ClientContext &context) const;

public:
	//! The table definition as returned by Glue when the entry was created
	GlueTableInfo table_info;

private:
	//! The columns of the table: data columns first, partition keys last
	ColumnList columns;
};

} // namespace duckdb
