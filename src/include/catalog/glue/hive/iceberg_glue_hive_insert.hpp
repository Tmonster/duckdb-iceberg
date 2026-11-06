#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {
class IcebergGlueHiveTable;

//! Writes rows into a Hive table of a Glue catalog: a COPY into the table location (one <key=value> directory level
//! per partition key, or the location of an existing partition), then the new partition directories are registered in
//! Glue. Ported from the aws_glue extension. Glue has no transactions: the files and partitions are published when the
//! statement finishes, a ROLLBACK doesn't undo them.
class IcebergGlueHiveInsert : public PhysicalOperator {
public:
	IcebergGlueHiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, IcebergGlueHiveTable &table);

	//! INSERT INTO <hive table>
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                                    IcebergGlueHiveTable &table, optional_ptr<PhysicalOperator> plan);

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	static PhysicalOperator &PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
	                                   IcebergGlueHiveTable &table, PhysicalOperator &plan, const vector<Identifier> &names,
	                                   const vector<LogicalType> &types);

public:
	//! The table written to
	IcebergGlueHiveTable &table;
		//! The directories of the existing partitions not laid out as <key>=<value>, with their Glue values
	unordered_map<string, vector<string>> partition_directories;
};

} // namespace duckdb
