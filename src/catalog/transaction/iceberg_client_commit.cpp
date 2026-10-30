#include "catalog/transaction/iceberg_client_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "common/iceberg_constants.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "core/metadata/sort/iceberg_sort_order.hpp"

namespace duckdb {

namespace {

//! Partition field ids start at 1000, so an unpartitioned table has last-partition-id 999
constexpr int32_t UNPARTITIONED_LAST_PARTITION_ID = 999;
constexpr const char *PREVIOUS_VERSIONS_MAX = "write.metadata.previous-versions-max";
constexpr idx_t DEFAULT_PREVIOUS_VERSIONS_MAX = 100;

//! A requirement that no longer holds means another commit changed the table first
[[noreturn]] void RequirementFailed(const string &message) {
	throw TransactionException("Conflict with a concurrent Iceberg commit, requirement failed: %s", message);
}

IcebergTableMetadata EmptyMetadata() {
	IcebergTableMetadata result(IcebergTableMetadataSchemas {});
	result.iceberg_version = 1;
	result.last_column_id = 0;
	result.last_partition_field_id = UNPARTITIONED_LAST_PARTITION_ID;
	result.default_spec_id = 0;
	result.default_sort_order_id = 0;
	result.last_sequence_number = 0;
	return result;
}

//! The ids added by the commit so far, for updates that refer to "the last added" one with -1
struct LastAdded {
	optional<int32_t> schema_id;
	optional<int32_t> spec_id;
	optional<int32_t> sort_order_id;
	vector<int64_t> snapshot_ids;
};

int32_t ResolveLastAdded(int32_t id, const optional<int32_t> &last_added, const char *kind) {
	if (id != -1) {
		return id;
	}
	if (!last_added) {
		throw InvalidInputException("Cannot set the last added %s: no %s was added in this commit", kind, kind);
	}
	return *last_added;
}

void ApplyAddSchema(IcebergTableMetadata &metadata, const rest_api_objects::AddSchemaUpdate &update,
                    LastAdded &last_added) {
	auto rest_schema = update.schema.Copy();
	if (!rest_schema.object_1.schema_id || *rest_schema.object_1.schema_id < 0) {
		int32_t next_id = 0;
		metadata.GetSchemas().ForEachSchema(
		    [&](const IcebergTableSchema &schema) { next_id = MaxValue(next_id, schema.schema_id + 1); });
		rest_schema.object_1.schema_id = next_id;
	}
	auto schema = IcebergTableSchema::ParseSchema(rest_schema);
	auto max_field_id = schema->GetMaxFieldId();
	auto &added = metadata.GetSchemasMutable().AddSchemaOrGetExisting(std::move(schema));
	last_added.schema_id = added.schema_id;

	int64_t last_column_id = metadata.HasLastColumnId() ? metadata.GetLastColumnId() : 0;
	last_column_id = MaxValue<int64_t>(last_column_id, max_field_id);
	if (update.last_column_id) {
		last_column_id = MaxValue<int64_t>(last_column_id, *update.last_column_id);
	}
	metadata.last_column_id = last_column_id;
}

void ApplyAddSpec(IcebergTableMetadata &metadata, const rest_api_objects::AddPartitionSpecUpdate &update,
                  LastAdded &last_added) {
	auto rest_spec = update.spec.Copy();
	if (!rest_spec.spec_id || *rest_spec.spec_id < 0) {
		int32_t next_id = 0;
		for (auto &entry : metadata.partition_specs) {
			next_id = MaxValue(next_id, entry.first + 1);
		}
		rest_spec.spec_id = next_id;
	}
	auto spec = IcebergPartitionSpec::ParseFromJson(rest_spec, metadata.iceberg_version);
	int32_t last_partition_id =
	    metadata.HasLastPartitionId() ? metadata.GetLastPartitionFieldId() : UNPARTITIONED_LAST_PARTITION_ID;
	for (auto &field : spec.fields) {
		last_partition_id = MaxValue<int32_t>(last_partition_id, NumericCast<int32_t>(field.partition_field_id));
	}
	metadata.last_partition_field_id = last_partition_id;

	auto spec_id = spec.spec_id;
	auto existing = metadata.partition_specs.find(spec_id);
	if (existing != metadata.partition_specs.end()) {
		if (!existing->second.Equals(spec)) {
			throw InvalidInputException("Cannot add partition spec %d: a different spec with this id exists", spec_id);
		}
	} else {
		metadata.partition_specs.emplace(spec_id, std::move(spec));
	}
	last_added.spec_id = spec_id;
}

void ApplyAddSortOrder(IcebergTableMetadata &metadata, const rest_api_objects::AddSortOrderUpdate &update,
                       LastAdded &last_added) {
	auto sort_order = IcebergSortOrder::ParseFromJson(update.sort_order);
	if (sort_order.sort_order_id < 0) {
		int32_t next_id = 1;
		for (auto &entry : metadata.sort_specs) {
			next_id = MaxValue(next_id, entry.first + 1);
		}
		sort_order.sort_order_id = next_id;
	}
	auto sort_order_id = sort_order.sort_order_id;
	auto existing = metadata.sort_specs.find(sort_order_id);
	if (existing != metadata.sort_specs.end()) {
		if (!existing->second.Equals(sort_order)) {
			throw InvalidInputException("Cannot add sort order %d: a different sort order with this id exists",
			                            sort_order_id);
		}
	} else {
		metadata.sort_specs.emplace(sort_order_id, std::move(sort_order));
	}
	last_added.sort_order_id = sort_order_id;
}

void ApplyAddSnapshot(IcebergTableMetadata &metadata, const rest_api_objects::AddSnapshotUpdate &update,
                      LastAdded &last_added) {
	auto &rest_snapshot = update.snapshot;
	auto snapshot_id = rest_snapshot.snapshot_id;
	if (metadata.snapshots.count(snapshot_id)) {
		throw InvalidInputException("Cannot add snapshot %d: a snapshot with this id already exists", snapshot_id);
	}
	auto snapshot = IcebergSnapshot::ParseSnapshot(rest_snapshot, metadata);
	if (metadata.iceberg_version >= 2) {
		if (!snapshot.sequence_number) {
			throw InvalidInputException("Cannot add snapshot %d: it has no sequence number", snapshot_id);
		}
		if (*snapshot.sequence_number <= metadata.last_sequence_number) {
			RequirementFailed(StringUtil::Format("snapshot %d has sequence number %d, which is not greater than the "
			                                     "table's last sequence number %d",
			                                     snapshot_id, *snapshot.sequence_number,
			                                     metadata.last_sequence_number));
		}
		metadata.last_sequence_number = *snapshot.sequence_number;
	}
	if (metadata.iceberg_version >= 3) {
		if (!snapshot.first_row_id || !snapshot.added_rows) {
			throw InvalidInputException("Cannot add snapshot %d: v3 snapshots require 'first-row-id' and 'added-rows'",
			                            snapshot_id);
		}
		auto next_row_id = metadata.next_row_id ? *metadata.next_row_id : 0;
		if (*snapshot.first_row_id < next_row_id) {
			RequirementFailed(StringUtil::Format("snapshot %d assigns row ids from %d, but row ids up to %d are "
			                                     "already assigned",
			                                     snapshot_id, *snapshot.first_row_id, next_row_id));
		}
		metadata.next_row_id = *snapshot.first_row_id + *snapshot.added_rows;
	}
	metadata.snapshots.emplace(snapshot_id, std::move(snapshot));
	last_added.snapshot_ids.push_back(snapshot_id);
}

void ApplySetSnapshotRef(IcebergTableMetadata &metadata, const rest_api_objects::SetSnapshotRefUpdate &update,
                         const LastAdded &last_added, timestamp_ms_t commit_time) {
	auto &reference = update.snapshot_reference;
	auto snapshot = metadata.snapshots.find(reference.snapshot_id);
	if (snapshot == metadata.snapshots.end()) {
		throw InvalidInputException("Cannot set ref '%s' to unknown snapshot %d", update.ref_name,
		                            reference.snapshot_id);
	}
	if (!metadata.refs) {
		metadata.refs = rest_api_objects::SnapshotReferences();
	}
	metadata.refs->additional_properties[update.ref_name] = reference.Copy();
	if (update.ref_name != "main") {
		return;
	}
	if (metadata.current_snapshot_id && *metadata.current_snapshot_id == reference.snapshot_id) {
		return;
	}
	auto &added = last_added.snapshot_ids;
	bool added_by_commit = std::find(added.begin(), added.end(), reference.snapshot_id) != added.end();
	metadata.snapshot_log.emplace_back(reference.snapshot_id,
	                                   added_by_commit ? snapshot->second.timestamp_ms : commit_time);
	metadata.current_snapshot_id = reference.snapshot_id;
}

void ApplyUpdate(IcebergTableMetadata &metadata, const rest_api_objects::TableUpdate &update, bool creating,
                 LastAdded &last_added, timestamp_ms_t commit_time) {
	if (update.assign_uuidupdate) {
		auto &uuid = update.assign_uuidupdate->uuid;
		if (creating && metadata.table_uuid.empty()) {
			metadata.table_uuid = uuid.empty() ? UUID::ToString(UUID::GenerateRandomUUID()) : uuid;
		} else if (!uuid.empty() && uuid != metadata.table_uuid) {
			throw InvalidInputException("Cannot reassign the table UUID '%s' to '%s'", metadata.table_uuid, uuid);
		}
	} else if (update.upgrade_format_version_update) {
		auto format_version = update.upgrade_format_version_update->format_version;
		if (format_version > MAX_ICEBERG_FORMAT_VERSION) {
			throw NotImplementedException("Iceberg format-version %d is not supported", format_version);
		}
		if (format_version < metadata.iceberg_version) {
			throw InvalidInputException("Cannot downgrade the format-version from %d to %d", metadata.iceberg_version,
			                            format_version);
		}
		if (format_version >= 3 && !metadata.next_row_id) {
			metadata.next_row_id = 0;
		}
		metadata.iceberg_version = format_version;
	} else if (update.add_schema_update) {
		ApplyAddSchema(metadata, *update.add_schema_update, last_added);
	} else if (update.set_current_schema_update) {
		auto schema_id =
		    ResolveLastAdded(update.set_current_schema_update->schema_id, last_added.schema_id, "schema");
		// Throws if the schema doesn't exist
		(void)metadata.GetSchemaFromId(schema_id);
		metadata.SetCurrentSchemaId(schema_id);
	} else if (update.add_partition_spec_update) {
		ApplyAddSpec(metadata, *update.add_partition_spec_update, last_added);
	} else if (update.set_default_spec_update) {
		auto spec_id = ResolveLastAdded(update.set_default_spec_update->spec_id, last_added.spec_id, "partition spec");
		if (!metadata.partition_specs.count(spec_id)) {
			throw InvalidInputException("Cannot set the default partition spec to unknown spec %d", spec_id);
		}
		metadata.default_spec_id = spec_id;
	} else if (update.add_sort_order_update) {
		ApplyAddSortOrder(metadata, *update.add_sort_order_update, last_added);
	} else if (update.set_default_sort_order_update) {
		auto sort_order_id = ResolveLastAdded(update.set_default_sort_order_update->sort_order_id,
		                                      last_added.sort_order_id, "sort order");
		if (!metadata.sort_specs.count(sort_order_id)) {
			throw InvalidInputException("Cannot set the default sort order to unknown sort order %d", sort_order_id);
		}
		metadata.default_sort_order_id = sort_order_id;
	} else if (update.add_snapshot_update) {
		ApplyAddSnapshot(metadata, *update.add_snapshot_update, last_added);
	} else if (update.set_snapshot_ref_update) {
		ApplySetSnapshotRef(metadata, *update.set_snapshot_ref_update, last_added, commit_time);
	} else if (update.set_properties_update) {
		for (auto &property : update.set_properties_update->updates) {
			metadata.table_properties[property.first] = property.second;
		}
	} else if (update.remove_properties_update) {
		for (auto &property : update.remove_properties_update->removals) {
			metadata.table_properties.erase(property);
		}
	} else if (update.set_location_update) {
		metadata.location = update.set_location_update->location;
		StringUtil::RTrim(metadata.location, "/");
	} else {
		JSONWriter writer;
		writer.SetRoot(update.ToJSON(writer));
		throw NotImplementedException("Client-side Iceberg commits don't support the table update %s",
		                              writer.ToString());
	}
}

//! Like a catalog server, reject metadata whose current partition spec or sort order refers to a column that is not
//! in the current schema, e.g. after a concurrent commit partitioned the table by a column this commit drops
void ValidateMetadata(const IcebergTableMetadata &metadata) {
	auto &schema = metadata.GetSchemaFromId(metadata.GetCurrentSchemaId());
	auto check_source = [&](uint64_t source_id, const IcebergTransform &transform, const char *kind, int64_t id) {
		if (transform.RawType() == "void") {
			return;
		}
		if (!schema.TryGetColumnByFieldId(source_id)) {
			throw InvalidInputException("Cannot commit table metadata: the default %s %d refers to field %d, which is "
			                            "not in the current schema",
			                            kind, id, source_id);
		}
	};
	auto spec = metadata.partition_specs.find(metadata.default_spec_id);
	if (spec != metadata.partition_specs.end()) {
		for (auto &field : spec->second.fields) {
			check_source(field.source_id, field.transform, "partition spec", spec->first);
		}
	}
	if (metadata.default_sort_order_id.IsValid()) {
		auto sort_order = metadata.sort_specs.find(NumericCast<int32_t>(metadata.default_sort_order_id.GetIndex()));
		if (sort_order != metadata.sort_specs.end()) {
			for (auto &field : sort_order->second.fields) {
				check_source(NumericCast<uint64_t>(field.source_id), field.transform, "sort order", sort_order->first);
			}
		}
	}
}

} // namespace

void IcebergClientCommit::CheckRequirements(optional_ptr<const IcebergTableMetadata> base,
                                            const vector<rest_api_objects::TableRequirement> &requirements) {
	for (auto &requirement : requirements) {
		if (requirement.assert_create) {
			if (base) {
				RequirementFailed("assert-create, the table already exists");
			}
			continue;
		}
		if (!base) {
			RequirementFailed("the table does not exist");
		}
		auto &metadata = *base;
		if (auto &assert_uuid = requirement.assert_table_uuid) {
			if (assert_uuid->uuid != metadata.table_uuid) {
				RequirementFailed(StringUtil::Format("assert-table-uuid, expected '%s', found '%s'", assert_uuid->uuid,
				                                     metadata.table_uuid));
			}
		} else if (auto &assert_ref = requirement.assert_ref_snapshot_id) {
			optional<int64_t> current;
			if (metadata.refs) {
				auto ref = metadata.refs->additional_properties.find(assert_ref->ref);
				if (ref != metadata.refs->additional_properties.end()) {
					current = ref->second.snapshot_id;
				}
			} else if (assert_ref->ref == "main") {
				// Older metadata only records the main branch as current-snapshot-id
				current = metadata.current_snapshot_id;
			}
			if (current != assert_ref->snapshot_id) {
				auto to_string = [](const optional<int64_t> &id) {
					return id ? std::to_string(*id) : string("no snapshot");
				};
				RequirementFailed(StringUtil::Format("assert-ref-snapshot-id for ref '%s', expected %s, found %s",
				                                     assert_ref->ref, to_string(assert_ref->snapshot_id),
				                                     to_string(current)));
			}
		} else if (auto &assert_field_id = requirement.assert_last_assigned_field_id) {
			int64_t current = metadata.HasLastColumnId() ? NumericCast<int64_t>(metadata.GetLastColumnId()) : 0;
			if (current != assert_field_id->last_assigned_field_id) {
				RequirementFailed(StringUtil::Format("assert-last-assigned-field-id, expected %d, found %d",
				                                     assert_field_id->last_assigned_field_id, current));
			}
		} else if (auto &assert_schema_id = requirement.assert_current_schema_id) {
			if (assert_schema_id->current_schema_id != metadata.GetCurrentSchemaId()) {
				RequirementFailed(StringUtil::Format("assert-current-schema-id, expected %d, found %d",
				                                     assert_schema_id->current_schema_id,
				                                     metadata.GetCurrentSchemaId()));
			}
		} else if (auto &assert_partition_id = requirement.assert_last_assigned_partition_id) {
			int32_t current =
			    metadata.HasLastPartitionId() ? metadata.GetLastPartitionFieldId() : UNPARTITIONED_LAST_PARTITION_ID;
			if (current != assert_partition_id->last_assigned_partition_id) {
				RequirementFailed(StringUtil::Format("assert-last-assigned-partition-id, expected %d, found %d",
				                                     assert_partition_id->last_assigned_partition_id, current));
			}
		} else if (auto &assert_spec_id = requirement.assert_default_spec_id) {
			if (assert_spec_id->default_spec_id != metadata.default_spec_id) {
				RequirementFailed(StringUtil::Format("assert-default-spec-id, expected %d, found %d",
				                                     assert_spec_id->default_spec_id, metadata.default_spec_id));
			}
		} else if (auto &assert_sort_order_id = requirement.assert_default_sort_order_id) {
			int64_t current = metadata.default_sort_order_id.IsValid()
			                      ? NumericCast<int64_t>(metadata.default_sort_order_id.GetIndex())
			                      : 0;
			if (assert_sort_order_id->default_sort_order_id != current) {
				RequirementFailed(StringUtil::Format("assert-default-sort-order-id, expected %d, found %d",
				                                     assert_sort_order_id->default_sort_order_id, current));
			}
		} else {
			throw NotImplementedException("Client-side Iceberg commits don't support this table requirement");
		}
	}
}

IcebergTableMetadata IcebergClientCommit::Apply(optional_ptr<const IcebergTableMetadata> base,
                                                const rest_api_objects::CommitTableRequest &request) {
	CheckRequirements(base, request.requirements);

	const bool creating = !base;
	auto result = creating ? EmptyMetadata() : base->Copy();
	auto commit_time = timestamp_ms_t(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()));
	LastAdded last_added;
	for (auto &update : request.updates) {
		ApplyUpdate(result, update, creating, last_added, commit_time);
	}
	if (creating) {
		if (result.table_uuid.empty()) {
			result.table_uuid = UUID::ToString(UUID::GenerateRandomUUID());
		}
		if (result.location.empty()) {
			throw InvalidInputException("Cannot create a table without a location");
		}
		if (result.GetSchemas().IsEmpty()) {
			throw InvalidInputException("Cannot create a table without a schema");
		}
		if (result.partition_specs.empty()) {
			result.partition_specs.emplace(0, IcebergPartitionSpec(0));
		}
		if (result.sort_specs.empty()) {
			result.sort_specs.emplace(0, IcebergSortOrder(0));
		}
	}
	// last-updated-ms must not go backwards, and should not precede the snapshots it records
	auto last_updated = commit_time;
	if (base && base->last_updated_ms.value > last_updated.value) {
		last_updated = base->last_updated_ms;
	}
	for (auto &snapshot_id : last_added.snapshot_ids) {
		auto &snapshot = result.snapshots.at(snapshot_id);
		if (snapshot.timestamp_ms.value > last_updated.value) {
			last_updated = snapshot.timestamp_ms;
		}
	}
	result.last_updated_ms = last_updated;
	ValidateMetadata(result);
	return result;
}

idx_t IcebergClientCommit::ParseMetadataVersion(const string &metadata_location) {
	auto file_name = metadata_location.substr(metadata_location.find_last_of("/\\") + 1);
	idx_t start = 0;
	if (StringUtil::StartsWith(file_name, "v")) {
		// 'v<version>.metadata.json', written by the Hadoop catalog and DuckDB's COPY TO
		start = 1;
	}
	idx_t end = start;
	while (end < file_name.size() && StringUtil::CharacterIsDigit(file_name[end])) {
		end++;
	}
	if (end == start || end - start > 18) {
		return 0;
	}
	if (start == 1) {
		if (file_name[end] != '.') {
			return 0;
		}
	} else {
		// '<version>-<uuid>': a name that is only a UUID can also start with digits followed by a dash
		static constexpr idx_t UUID_LENGTH = 36;
		auto uuid_start = end + 1;
		if (file_name[end] != '-' || file_name.size() < uuid_start + UUID_LENGTH ||
		    file_name[uuid_start + UUID_LENGTH] != '.') {
			return 0;
		}
		for (idx_t dash : {8, 13, 18, 23}) {
			if (file_name[uuid_start + dash] != '-') {
				return 0;
			}
		}
	}
	return std::stoull(file_name.substr(start, end - start));
}

string IcebergClientCommit::WriteMetadataFile(ClientContext &context, IcebergTableMetadata &metadata,
                                              optional_ptr<const IcebergTableMetadata> previous,
                                              const string &previous_metadata_location,
                                              const string &metadata_directory) {
	idx_t version = 0;
	if (previous && !previous_metadata_location.empty()) {
		version = ParseMetadataVersion(previous_metadata_location) + 1;
		// Each entry is a metadata file and the time it became the table's current metadata
		metadata.metadata_log.emplace_back(previous_metadata_location, previous->last_updated_ms);
	}
	auto max_previous_versions = DEFAULT_PREVIOUS_VERSIONS_MAX;
	auto max_previous_versions_property = metadata.GetTableProperty(PREVIOUS_VERSIONS_MAX);
	if (!max_previous_versions_property.empty()) {
		max_previous_versions = MaxValue<idx_t>(1, std::stoull(max_previous_versions_property));
	}
	if (metadata.metadata_log.size() > max_previous_versions) {
		auto excess = metadata.metadata_log.size() - max_previous_versions;
		metadata.metadata_log.erase(metadata.metadata_log.begin(),
		                            metadata.metadata_log.begin() + NumericCast<int64_t>(excess));
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto file_name = StringUtil::Format("%05d-%s.metadata.json", version, UUID::ToString(UUID::GenerateRandomUUID()));
	auto path = fs.JoinPath(metadata_directory, file_name);
	metadata.WriteMetadata(context, path);
	return path;
}

vector<string> IcebergClientCommit::Compare(const IcebergTableMetadata &expected, const IcebergTableMetadata &actual) {
	vector<string> differences;
	auto check = [&](bool equal, const string &field, const string &expected_value, const string &actual_value) {
		if (!equal) {
			differences.push_back(
			    StringUtil::Format("%s: expected %s, found %s", field, expected_value, actual_value));
		}
	};
	auto optional_to_string = [](const optional<int64_t> &value) {
		return value ? std::to_string(*value) : string("(none)");
	};
	auto optional_idx_to_string = [](const optional_idx &value) {
		return value.IsValid() ? std::to_string(value.GetIndex()) : string("(none)");
	};

	check(expected.iceberg_version == actual.iceberg_version, "format-version", std::to_string(expected.iceberg_version),
	      std::to_string(actual.iceberg_version));
	check(expected.table_uuid == actual.table_uuid, "table-uuid", expected.table_uuid, actual.table_uuid);
	check(expected.location == actual.location, "location", expected.location, actual.location);
	check(expected.last_sequence_number == actual.last_sequence_number, "last-sequence-number",
	      std::to_string(expected.last_sequence_number), std::to_string(actual.last_sequence_number));
	check(expected.last_column_id == actual.last_column_id, "last-column-id",
	      optional_idx_to_string(expected.last_column_id), optional_idx_to_string(actual.last_column_id));
	check(expected.last_partition_field_id == actual.last_partition_field_id, "last-partition-id",
	      optional_idx_to_string(expected.last_partition_field_id),
	      optional_idx_to_string(actual.last_partition_field_id));
	check(expected.GetCurrentSchemaId() == actual.GetCurrentSchemaId(), "current-schema-id",
	      std::to_string(expected.GetCurrentSchemaId()), std::to_string(actual.GetCurrentSchemaId()));
	check(expected.default_spec_id == actual.default_spec_id, "default-spec-id",
	      std::to_string(expected.default_spec_id), std::to_string(actual.default_spec_id));
	check(expected.default_sort_order_id == actual.default_sort_order_id, "default-sort-order-id",
	      optional_idx_to_string(expected.default_sort_order_id),
	      optional_idx_to_string(actual.default_sort_order_id));
	check(expected.current_snapshot_id == actual.current_snapshot_id, "current-snapshot-id",
	      optional_to_string(expected.current_snapshot_id), optional_to_string(actual.current_snapshot_id));
	if (expected.iceberg_version >= 3) {
		check(expected.next_row_id == actual.next_row_id, "next-row-id", optional_to_string(expected.next_row_id),
		      optional_to_string(actual.next_row_id));
	}

	expected.GetSchemas().ForEachSchema([&](const IcebergTableSchema &schema) {
		auto field = StringUtil::Format("schema %d", schema.schema_id);
		bool found = false;
		actual.GetSchemas().ForEachSchema([&](const IcebergTableSchema &other) {
			if (other.schema_id == schema.schema_id) {
				found = true;
				check(schema.Equals(other), field, "a matching schema", "a different schema");
			}
		});
		check(found, field, "present", "missing");
	});
	for (auto &entry : expected.partition_specs) {
		auto field = StringUtil::Format("partition spec %d", entry.first);
		auto other = actual.partition_specs.find(entry.first);
		check(other != actual.partition_specs.end(), field, "present", "missing");
		if (other != actual.partition_specs.end()) {
			check(entry.second.Equals(other->second), field, entry.second.FieldsToJSONString(),
			      other->second.FieldsToJSONString());
		}
	}
	for (auto &entry : expected.sort_specs) {
		auto field = StringUtil::Format("sort order %d", entry.first);
		auto other = actual.sort_specs.find(entry.first);
		check(other != actual.sort_specs.end(), field, "present", "missing");
		if (other != actual.sort_specs.end()) {
			check(entry.second.Equals(other->second), field, "a matching sort order", "a different sort order");
		}
	}
	check(expected.snapshots.size() == actual.snapshots.size(), "snapshot count",
	      std::to_string(expected.snapshots.size()), std::to_string(actual.snapshots.size()));
	for (auto &entry : expected.snapshots) {
		auto field = StringUtil::Format("snapshot %d", entry.first);
		auto other = actual.snapshots.find(entry.first);
		check(other != actual.snapshots.end(), field, "present", "missing");
		if (other == actual.snapshots.end()) {
			continue;
		}
		auto &snapshot = entry.second;
		auto &other_snapshot = other->second;
		check(snapshot.sequence_number == other_snapshot.sequence_number, field + " sequence-number",
		      optional_to_string(snapshot.sequence_number), optional_to_string(other_snapshot.sequence_number));
		check(snapshot.parent_snapshot_id == other_snapshot.parent_snapshot_id, field + " parent-snapshot-id",
		      optional_to_string(snapshot.parent_snapshot_id), optional_to_string(other_snapshot.parent_snapshot_id));
		check(snapshot.manifest_list == other_snapshot.manifest_list, field + " manifest-list",
		      snapshot.manifest_list, other_snapshot.manifest_list);
		check(snapshot.first_row_id == other_snapshot.first_row_id, field + " first-row-id",
		      optional_to_string(snapshot.first_row_id), optional_to_string(other_snapshot.first_row_id));
		check(snapshot.added_rows == other_snapshot.added_rows, field + " added-rows",
		      optional_to_string(snapshot.added_rows), optional_to_string(other_snapshot.added_rows));
	}
	auto ref_snapshot = [](const IcebergTableMetadata &metadata, const string &ref) -> optional<int64_t> {
		if (!metadata.refs) {
			return optional<int64_t>();
		}
		auto entry = metadata.refs->additional_properties.find(ref);
		if (entry == metadata.refs->additional_properties.end()) {
			return optional<int64_t>();
		}
		return entry->second.snapshot_id;
	};
	if (expected.refs) {
		for (auto &entry : expected.refs->additional_properties) {
			check(entry.second.snapshot_id == ref_snapshot(actual, entry.first), "ref " + entry.first,
			      std::to_string(entry.second.snapshot_id), optional_to_string(ref_snapshot(actual, entry.first)));
		}
	}
	vector<int64_t> expected_log;
	vector<int64_t> actual_log;
	for (auto &entry : expected.snapshot_log) {
		expected_log.push_back(entry.first);
	}
	for (auto &entry : actual.snapshot_log) {
		actual_log.push_back(entry.first);
	}
	auto log_to_string = [](const vector<int64_t> &log) {
		return StringUtil::Join(log, ", ", [](int64_t snapshot_id) { return std::to_string(snapshot_id); });
	};
	check(expected_log == actual_log, "snapshot-log", log_to_string(expected_log), log_to_string(actual_log));
	// A catalog may add properties of its own, but must retain the ones the commit set
	for (auto &property : expected.table_properties) {
		auto other = actual.table_properties.find(property.first);
		check(other != actual.table_properties.end() && other->second == property.second,
		      "property '" + property.first + "'", property.second,
		      other == actual.table_properties.end() ? string("(missing)") : other->second);
	}
	return differences;
}

} // namespace duckdb
