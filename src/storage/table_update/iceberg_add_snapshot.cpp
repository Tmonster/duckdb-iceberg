#include "storage/table_update/iceberg_add_snapshot.hpp"

#include "../../include/metadata/iceberg_manifest_list.hpp"
#include "../../include/metadata/iceberg_snapshot.hpp"
#include "storage/irc_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
                                       const string &manifest_list_path, IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_file(std::move(manifest_file)),
      manifest_list(manifest_list_path), snapshot(std::move(snapshot)) {
}

rest_api_objects::TableUpdate CreateAddSnapshotUpdate(const IcebergSnapshot &snapshot) {
	rest_api_objects::TableUpdate table_update;

	table_update.has_add_snapshot_update = true;
	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject();
	return table_update;
}

void IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	D_ASSERT(!manifest_file.data_files.empty());

	auto manifest_length = manifest_file::WriteToFile(table_info, manifest_file, avro_copy, db, context);
	manifest.manifest_length = manifest_length;

	D_ASSERT(manifest_list.manifests.empty());
	// if this is a delete snapshot, there is more involved.
	// we need to add all manifests where not a single delete is updated
	//    this will include equality delete files
	// if a delete file has been updated, we need to rewrite all entries in that manifest list.
	///
	if (snapshot.operation == IcebergSnapshotOperationType::DELETE) {
		// only copy
		for (auto &prev_manifest : commit_state.manifests) {
			if (prev_manifest.content == IcebergManifestContentType::DATA) {
				manifest_list.manifests.push_back(std::move(prev_manifest));
			}
		}
		// now push back this manifest (which contains delete info)
		manifest_list.manifests.push_back(std::move(manifest));
		manifest_list::WriteToFile(manifest_list, avro_copy, db, context);
		commit_state.manifests = std::move(manifest_list.manifests);
		commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(snapshot));
		return;
	}
	// here we need to perform a check on what manifests we want to copy over.
	// for delete snapshots, we don't want to copy manifests, or the manifest lists over
	// we are overwriting the delete files, so they are outdated, and we can just write the parquet
	// files in the new manifest list, and that is also fine.
	manifest_list.manifests = std::move(commit_state.manifests);
	manifest_list.manifests.push_back(std::move(manifest));
	manifest_list::WriteToFile(manifest_list, avro_copy, db, context);
	commit_state.manifests = std::move(manifest_list.manifests);

	commit_state.table_change.updates.push_back(CreateAddSnapshotUpdate(snapshot));
}

} // namespace duckdb
