//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/iceberg_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "irc_transaction.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/irc_table_entry.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

struct IcebergCopyOptions {
	IcebergCopyOptions(unique_ptr<CopyInfo> info, CopyFunction copy_function);

	unique_ptr<CopyInfo> info;
	CopyFunction copy_function;
	unique_ptr<FunctionData> bind_data;

	string file_path;
	bool use_tmp_file;
	FilenamePattern filename_pattern;
	string file_extension;
	CopyOverwriteMode overwrite_mode;
	bool per_thread_output;
	optional_idx file_size_bytes;
	bool rotate;
	CopyFunctionReturnType return_type;
	bool hive_file_pattern;

	bool partition_output;
	bool write_partition_columns;
	bool write_empty_file = true;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

	//! Set of projection columns to execute prior to inserting (if any)
	vector<unique_ptr<Expression>> projection_list;
};

struct IcebergCopyInput {
	explicit IcebergCopyInput(ClientContext &context, ICTableEntry &table);
	// IcebergCopyInput(ClientContext &context, IRCSchemaEntry &schema, const ColumnList &columns,
	//                  const string &data_path_p);

	IRCatalog &catalog;
	// Is table needed? maybe I can get away with just partition data.
	ICTableEntry &table;
	optional_ptr<IcebergPartitionSpec> partition_data;
	vector<Value> field_data;
	const ColumnList &columns;
	const string data_path;
	// InsertVirtualColumns virtual_columns = InsertVirtualColumns::NONE;
	// optional_idx get_table_index;
};

class IcebergInsertGlobalState : public GlobalSinkState {
public:
	explicit IcebergInsertGlobalState(ClientContext &context);
	ClientContext &context;
	mutex lock;
	vector<IcebergManifestEntry> written_files;
	atomic<idx_t> insert_count;
};

struct IcebergColumnStats {
	explicit IcebergColumnStats(LogicalType type_p) : type(std::move(type_p)) {
	}

	// Copy constructor
	IcebergColumnStats(const IcebergColumnStats &other);
	IcebergColumnStats &operator=(const IcebergColumnStats &other);
	IcebergColumnStats(IcebergColumnStats &&other) noexcept = default;
	IcebergColumnStats &operator=(IcebergColumnStats &&other) noexcept = default;

	LogicalType type;
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
	bool has_column_size_bytes = false;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const IcebergColumnStats &new_stats);
	IcebergColumnStats Copy() const;

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

class IcebergInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	              physical_index_vector_t<idx_t> column_index_map);
	IcebergInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, TableCatalogEntry &table);

	//! CREATE TABLE AS
	IcebergInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	              unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;
	//! The physical copy used internally by this insert
	unique_ptr<PhysicalOperator> physical_copy_to_file;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	static IcebergCopyOptions GetCopyOptions(ClientContext &context, IcebergCopyInput &copy_input);
	static PhysicalOperator &PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           IcebergCopyInput &copy_input, optional_ptr<PhysicalOperator> plan);

	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, ICTableEntry &table);
	static vector<IcebergManifestEntry> GetInsertManifestEntries(IcebergInsertGlobalState &global_state);
	static IcebergColumnStats ParseColumnStats(const LogicalType &type, const vector<Value> &col_stats,
	                                           ClientContext &context);
	static void AddWrittenFiles(IcebergInsertGlobalState &global_state, DataChunk &chunk,
	                            optional_ptr<TableCatalogEntry> table);

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
