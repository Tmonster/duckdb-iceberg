//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//! Walks the post-built-in-optimizer plan and re-introduces equality-delete columns onto every
//! iceberg_scan LogicalGet's column_ids. Built-in passes (filter pushdown + RemoveUnusedColumns)
//! often prune those columns when the user's projection doesn't reference them — only a
//! TableFilter does — and the runtime equality-delete machinery in IcebergMultiFileReader needs
//! them materialized in the scan chunk to evaluate `(col != value) OR ...`.
class IcebergRequiredColumnInjector : public LogicalOperatorVisitor {
public:
	void VisitOperator(LogicalOperator &op) override;

private:
	static void RewriteIcebergScan(LogicalGet &get);
};

//! Optimizer-extension factory. Registered from `iceberg_extension.cpp:Load()` so this visitor
//! runs after every built-in optimizer pass on every query.
struct IcebergOptimizerExtension {
	static OptimizerExtension Create();
};

} // namespace duckdb
