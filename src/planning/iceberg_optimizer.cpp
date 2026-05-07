#include "planning/iceberg_optimizer.hpp"

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"

#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

void IcebergRequiredColumnInjector::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		// Cheap discriminator; the bind-data dynamic_cast inside RewriteIcebergScan is the
		// authoritative check (and bails out cleanly for non-iceberg LogicalGets).
		if (get.function.name == "iceberg_scan") {
			RewriteIcebergScan(get);
		}
	}
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void IcebergRequiredColumnInjector::RewriteIcebergScan(LogicalGet &get) {
	if (!get.bind_data) {
		return;
	}
	auto &mfbd = get.bind_data->Cast<MultiFileBindData>();
	if (!mfbd.file_list) {
		return;
	}
	auto iceberg_list = dynamic_cast<IcebergMultiFileList *>(mfbd.file_list.get());
	if (!iceberg_list) {
		return;
	}

	{
		lock_guard<mutex> guard(iceberg_list->delete_lock);
		iceberg_list->EnumerateDeleteManifestEntries();
	}

	unordered_set<int32_t> required_field_ids;
	for (auto &entry : iceberg_list->delete_manifest_entries) {
		auto &mft = entry.entry;
		if (mft.data_file.content != IcebergManifestEntryContentType::EQUALITY_DELETES) {
			continue;
		}
		for (auto fid : mft.data_file.equality_ids) {
			required_field_ids.insert(fid);
		}
	}
	if (required_field_ids.empty()) {
		return;
	}

	auto &iceberg_schema = iceberg_list->GetSchema().columns;
	const auto &existing = get.GetColumnIds();
	for (idx_t i = 0; i < iceberg_schema.size(); i++) {
		if (!required_field_ids.count(iceberg_schema[i]->id)) {
			continue;
		}
		bool already_present = false;
		for (auto &existing_idx : existing) {
			if (!existing_idx.IsVirtualColumn() && existing_idx.GetPrimaryIndex() == i) {
				already_present = true;
				break;
			}
		}
		if (!already_present) {
			get.AddColumnId(i);
		}
	}
}

static void IcebergOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	IcebergRequiredColumnInjector visitor;
	visitor.VisitOperator(*plan);
}

OptimizerExtension IcebergOptimizerExtension::Create() {
	OptimizerExtension ext;
	ext.optimize_function = IcebergOptimize;
	return ext;
}

} // namespace duckdb
