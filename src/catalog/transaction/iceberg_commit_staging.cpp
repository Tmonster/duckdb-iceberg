#include "catalog/transaction/iceberg_commit_staging.hpp"

#include "common/iceberg_constants.hpp"
#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "catalog/rest/api/iceberg_table_update.hpp"
#include "catalog/rest/api/table_update.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"

namespace duckdb {

static rest_api_objects::TableRequirement CreateAssertRefSnapshotIdRequirement(const IcebergSnapshot &old_snapshot) {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.snapshot_id = old_snapshot.snapshot_id;
	res.type = "assert-ref-snapshot-id";
	return req;
}

static rest_api_objects::TableRequirement CreateAssertNoSnapshotRequirement() {
	rest_api_objects::TableRequirement req;
	req.assert_ref_snapshot_id = rest_api_objects::AssertRefSnapshotId();

	auto &res = *req.assert_ref_snapshot_id;
	res.ref = "main";
	res.type = "assert-ref-snapshot-id";
	return req;
}

static bool NeedsAssertSchemaId(const IcebergTransactionData &transaction_data, const IcebergTable &table_info) {
	(void)table_info;
	return transaction_data.assert_schema_id;
}


static void CreateTableRequirements(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state,
                                    const IcebergTransactionData &transaction_data,
                                    const optional_ptr<const IcebergSnapshot> &current_snapshot) {
	const bool has_assert_create = transaction_data.has_assert_create;
	for (auto &requirement : transaction_data.requirements) {
		requirement->CreateRequirement(db, context, commit_state);
	}
	if (!has_assert_create && NeedsAssertSchemaId(transaction_data, commit_state.table_info)) {
		AssertCurrentSchemaIdRequirement requirement(transaction_data.initial_schema_id);
		requirement.CreateRequirement(db, context, commit_state);
	}
	if (!has_assert_create && commit_state.table_info.HasTransactionUpdates()) {
		auto uuid_requirement = AssertTableUUIDRequirement(transaction_data.initial_table_uuid);
		uuid_requirement.CreateRequirement(db, context, commit_state);
	}
	if (current_snapshot && !transaction_data.alters.empty()) {
		commit_state.table_change.requirements.push_back(CreateAssertRefSnapshotIdRequirement(*current_snapshot));
	} else if (!current_snapshot && !transaction_data.alters.empty() && !has_assert_create) {
		commit_state.table_change.requirements.push_back(CreateAssertNoSnapshotRequirement());
	}
}

//! Safe to re-apply a DELETE on retry only if every commit in (base, tip] was a
//! pure append; anything else may have removed/rewritten the target data. Ranges
//! over sequence numbers (parent links aren't populated on read snapshots). A
//! scan/tip snapshot the catalog doesn't expose (e.g. no history) can't be
//! proven safe, so it counts as not reappliable.
static bool DeleteCanReapply(const IcebergTableMetadata &metadata, int64_t base_snapshot_id, int64_t tip_snapshot_id) {
	if (base_snapshot_id == tip_snapshot_id) {
		return true;
	}
	auto base = metadata.FindSnapshotByIdInternal(base_snapshot_id);
	auto tip = metadata.FindSnapshotByIdInternal(tip_snapshot_id);
	if (!base || !tip) {
		return false;
	}
	if (!base->sequence_number || !tip->sequence_number) {
		throw InvalidConfigurationException("Committed snapshot is missing a sequence number");
	}
	int64_t base_sequence = *base->sequence_number;
	int64_t tip_sequence = *tip->sequence_number;
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		if (!snapshot.sequence_number) {
			throw InvalidConfigurationException("Committed snapshot is missing a sequence number");
		}
		int64_t sequence = *snapshot.sequence_number;
		if (sequence <= base_sequence || sequence > tip_sequence) {
			continue;
		}
		if (snapshot.operation != IcebergSnapshotOperationType::APPEND) {
			return false;
		}
	}
	return true;
}

//! Throw if a retried DELETE can't be safely re-applied. No-op on the first
//! attempt (tip == scan) and for non-delete transactions.
static void VerifyDeleteRetryability(const IcebergTable &table_info,
                                     optional_ptr<const IcebergSnapshot> current_snapshot) {
	if (!table_info.transaction_data) {
		return;
	}
	auto &transaction_data = *table_info.transaction_data;
	if (!transaction_data.ContainsDelete()) {
		return;
	}
	//! No base snapshot: the delete targets data created in this transaction, so there is no
	//! committed base to rebase against and nothing to verify.
	if (!transaction_data.base_snapshot_id) {
		return;
	}
	if (!current_snapshot) {
		return;
	}
	auto scan_snapshot_id = *transaction_data.base_snapshot_id;
	auto tip_snapshot_id = current_snapshot->snapshot_id;

	//! Nothing to reconcile if the tip hasn't moved since we scanned (e.g. the first attempt).
	if (scan_snapshot_id == tip_snapshot_id) {
		return;
	}

	//! Re-applying a DELETE over concurrent commits is opt-in: the table must explicitly request
	//! 'snapshot' isolation, and even then only a pure-append history is safe (DeleteCanReapply). Any
	//! other isolation - unset (Iceberg's default is 'serializable') or an explicit 'serializable' - must
	//! abort, since a concurrent append can add rows matching the delete predicate that a silent re-apply
	//! would leave behind.
	auto isolation_level = table_info.table_metadata.GetTableProperty(WRITE_DELETE_ISOLATION_LEVEL);
	if (StringUtil::CIEquals(isolation_level, "snapshot") &&
	    DeleteCanReapply(table_info.table_metadata, scan_snapshot_id, tip_snapshot_id)) {
		return;
	}
	throw TransactionException(
	    "DELETE on \"%s\" conflicts with a concurrent commit (scanned snapshot %s, now at %s); re-run the DELETE. "
	    "Set 'write.delete.isolation-level'='snapshot' to allow re-applying deletes over concurrent appends.",
	    table_info.name, std::to_string(scan_snapshot_id), std::to_string(tip_snapshot_id));
}

IcebergSingleTableStagedCommit IcebergCommitStaging::StageSingleTableCommit(DatabaseInstance &db, IcebergTable &table_info,
                                                                            ClientContext &context) {
	IcebergSingleTableStagedCommit info;
	IcebergCommitState commit_state(table_info, context);
	auto &table_change = commit_state.table_change;
	auto &schema = table_info.schema.Cast<IcebergSchemaEntry>();
	table_change.identifier = rest_api_objects::TableIdentifier();
	table_change.identifier->_namespace.value = schema.namespace_items;
	table_change.identifier->name = table_info.name;

	auto &metadata = commit_state.table_info.table_metadata;
	auto current_snapshot = metadata.GetLatestSnapshot();
	auto &transaction_data = *commit_state.table_info.transaction_data;
	transaction_data.VerifyMergeDeletesComplete();
	info.retryable = transaction_data.SupportsAppendRetry();
	info.retry_config = IcebergRetryConfig::FromTableMetadata(metadata);
	if (!transaction_data.alters.empty()) {
		table_info.LoadCredentials(context);
		commit_state.LoadExistingManifests(db, std::move(transaction_data.existing_manifest_list));
	}
	VerifyDeleteRetryability(commit_state.table_info, current_snapshot);

	for (auto &update : transaction_data.updates) {
		update->CreateUpdate(db, context, commit_state);
	}

	CreateTableRequirements(db, context, commit_state, transaction_data, current_snapshot);

	if (!transaction_data.alters.empty()) {
		auto &snapshot = *commit_state.GetLatestSnapshot();
		SetSnapshotRef set_snapshot_ref(snapshot.snapshot_id);
		set_snapshot_ref.CreateUpdate(db, context, commit_state);
	}

	info.created_metadata_files = std::move(commit_state.created_metadata_files);
	info.request = std::move(table_change);
	return info;
}

IcebergMultiTableStagedCommit IcebergCommitStaging::StageMultiTableCommit(DatabaseInstance &db,
                                                                          IcebergTransactionAlterUpdate &alter_update,
                                                                          ClientContext &context) {
	IcebergMultiTableStagedCommit info;
	auto &transaction = info.request;
	bool all_retryable = true;
	bool saw_table = false;
	for (auto &updated_table : alter_update.updated_tables) {
		auto &table_info = updated_table.second.get();
		if (!table_info.HasTransactionUpdates()) {
			//! No changes to commit
			continue;
		}

		auto table_transaction_info = StageSingleTableCommit(db, table_info, context);
		info.created_metadata_files.insert(info.created_metadata_files.end(),
		                                   table_transaction_info.created_metadata_files.begin(),
		                                   table_transaction_info.created_metadata_files.end());
		info.table_keys.insert(updated_table.first);
		transaction.table_changes.push_back(std::move(table_transaction_info.request));
		//! Tables in one atomic transaction share a single retry loop, so fold their retry policies
		//! into the most lenient one (first table seeds it, the rest are merged in).
		info.retry_config = saw_table ? info.retry_config.MostLenient(table_transaction_info.retry_config)
		                              : table_transaction_info.retry_config;
		saw_table = true;
		all_retryable = all_retryable && table_transaction_info.retryable;
	}
	info.retryable = saw_table && all_retryable;
	return info;
}

optional_ptr<IcebergTable> IcebergCommitStaging::GetSingleUpdatedTable(IcebergTransactionAlterUpdate &alter_update) {
	optional_ptr<IcebergTable> result;
	for (auto &entry : alter_update.updated_tables) {
		auto &table_info = entry.second.get();
		if (!table_info.HasTransactionUpdates()) {
			continue;
		}
		if (result) {
			throw InternalException("Single-table Iceberg commit staged multiple tables");
		}
		result = table_info;
	}
	return result;
}


} // namespace duckdb
