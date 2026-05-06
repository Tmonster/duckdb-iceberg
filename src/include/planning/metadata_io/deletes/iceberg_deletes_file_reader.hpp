//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/metadata_io/deletes/iceberg_deletes_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

// pass a open file info to the delete scan
struct IcebergDeleteScanInfo : public TableFunctionInfo {
public:
	IcebergDeleteScanInfo(OpenFileInfo file_info) : file_info(file_info) {
	}

public:
	OpenFileInfo file_info;
};

struct IcebergDeleteFileReader : public MultiFileReader {
	IcebergDeleteFileReader(shared_ptr<TableFunctionInfo> function_info);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override;

	unique_ptr<Expression>
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, idx_t &column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_idx,
	                           optional_ptr<MultiFileColumnDefinition> &global_column_reference) override;

	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

public:
	shared_ptr<TableFunctionInfo> function_info;
};

} // namespace duckdb
