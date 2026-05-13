//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planning/iceberg_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

//! Pre-optimizer that wraps every iceberg_scan LogicalGet whose underlying table has
//! equality-delete files in a LogicalFilter whose only condition is a call to the
//! no-op scalar function `verify_equality_delete_columns(col1, col2, ...)`. The
//! filter is a no-op at runtime (the function always returns true) but its arguments
//! reference the columns that equality-delete files need to match on, so DuckDB's
//! built-in column-pruning passes treat those columns as live and leave them in the
//! scan projection. This guarantees the equality-delete machinery in
//! IcebergMultiFileReader::FinalizeChunk can resolve its BoundReferenceExpression
//! indexes without a parallel column-index map.
struct IcebergOptimizerExtension {
	static OptimizerExtension Create();
};

} // namespace duckdb
