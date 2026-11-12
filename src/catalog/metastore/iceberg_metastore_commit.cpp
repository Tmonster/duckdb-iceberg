#include "catalog/metastore/iceberg_metastore_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <optional>

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/iceberg_view_entry.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "rest_catalog/objects/list.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "catalog/transaction/iceberg_commit_staging.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergMetastoreTransactionCommit::IcebergMetastoreTransactionCommit(IcebergMetastoreCatalog &catalog,
                                                                     IcebergTransaction &transaction)
    : catalog(catalog), transaction(transaction), db(transaction.db) {
}

namespace {

bool CreatesTable(const rest_api_objects::CommitTableRequest &request) {
	for (auto &requirement : request.requirements) {
		if (requirement.assert_create) {
			return true;
		}
	}
	return false;
}

} // namespace

namespace {

//! Failures that are certain not to have changed the metastore: a rejected requirement, a conflict, a missing or
//! existing entry, an unsupported operation. Anything else (a lost response, a server error) may have been applied.
bool IsDefinitiveFailure(const ErrorData &error) {
	switch (error.Type()) {
	case ExceptionType::TRANSACTION:
	case ExceptionType::CATALOG:
	case ExceptionType::CONSTRAINT:
	case ExceptionType::NOT_IMPLEMENTED:
	case ExceptionType::INVALID_INPUT:
	case ExceptionType::INVALID_CONFIGURATION:
	case ExceptionType::BINDER:
	case ExceptionType::PERMISSION:
		return true;
	default:
		return false;
	}
}

//! Test hook: 'lose_response' fails after an entry change (or Finish) succeeded, 'unreachable' also fails the check
//! of what was applied
string InjectedFault(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting(IcebergMetastoreTransactionCommit::FAULT_SETTING, setting) && !setting.IsNull()) {
		return setting.ToString();
	}
	return string();
}

} // namespace

IcebergMetastoreTransactionCommit::EntryState
IcebergMetastoreTransactionCommit::ReconcileEntry(ClientContext &context, const AttemptedEntry &entry) {
	if (InjectedFault(context) == "unreachable") {
		throw IOException("Injected fault: the metastore is unreachable");
	}
	auto current = catalog.GetMetadataLocation(context, entry.identifier, entry.type);
	if (current == entry.new_location) {
		return EntryState::APPLIED;
	}
	if (current == entry.base_location) {
		return EntryState::NOT_APPLIED;
	}
	return EntryState::UNKNOWN;
}

void IcebergMetastoreTransactionCommit::ThrowOutcomeUnknown(IcebergMetastoreCommit &commit, const ErrorData &error,
                                                            const string &what) {
	// The change may be visible: keep every file it may reference, and don't retry it
	commit.MarkPublished();
	throw IOException("Iceberg %s commit state unknown: %s failed with \"%s\", and whether it was applied could not "
	                  "be determined. The files the commit wrote are kept.",
	                  catalog.MetastoreName(), what, error.RawMessage());
}

void IcebergMetastoreTransactionCommit::PublishEntry(IcebergMetastoreCommit &commit, ClientContext &context,
                                                     const IcebergMetastoreIdentifier &identifier,
                                                     IcebergMetastoreEntryType type, const string &base_location,
                                                     const string &new_location, const std::function<void()> &change) {
	AttemptedEntry entry {identifier, type, base_location, new_location};
	attempted_entries.push_back(entry);
	try {
		change();
		if (!commit.IsAtomic() && !InjectedFault(context).empty()) {
			throw IOException("Injected fault: the response to the change of %s was lost", identifier.ToString());
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		// A metastore with transactions publishes nothing before Finish
		if (commit.IsAtomic() || IsDefinitiveFailure(error)) {
			throw;
		}
		EntryState state;
		try {
			state = ReconcileEntry(context, entry);
		} catch (std::exception &) {
			state = EntryState::UNKNOWN;
		}
		if (state == EntryState::NOT_APPLIED) {
			throw;
		}
		if (state == EntryState::UNKNOWN) {
			ThrowOutcomeUnknown(commit, error, "the change of " + identifier.ToString());
		}
		DUCKDB_LOG(context, IcebergLogType, "Iceberg %s change of %s was applied although it reported \"%s\"",
		           catalog.MetastoreName(), identifier.ToString(), error.RawMessage());
	}
	OnEntryChanged(commit);
}

void IcebergMetastoreTransactionCommit::FinishAttempt(IcebergMetastoreCommit &commit, ClientContext &context) {
	try {
		commit.Finish();
		if (commit.IsAtomic() && !InjectedFault(context).empty()) {
			throw IOException("Injected fault: the response to the commit was lost");
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		// A metastore without transactions published its changes as they were made
		if (!commit.IsAtomic() || IsDefinitiveFailure(error)) {
			throw;
		}
		// The database may have committed although the client saw an error (e.g. a lost connection)
		if (attempted_entries.empty()) {
			ThrowOutcomeUnknown(commit, error, "the commit");
		}
		idx_t applied = 0;
		idx_t not_applied = 0;
		try {
			for (auto &entry : attempted_entries) {
				auto state = ReconcileEntry(context, entry);
				applied += state == EntryState::APPLIED;
				not_applied += state == EntryState::NOT_APPLIED;
			}
		} catch (std::exception &) {
			ThrowOutcomeUnknown(commit, error, "the commit");
		}
		if (not_applied == attempted_entries.size()) {
			throw;
		}
		if (applied != attempted_entries.size()) {
			ThrowOutcomeUnknown(commit, error, "the commit");
		}
		DUCKDB_LOG(context, IcebergLogType, "Iceberg %s commit was applied although it reported \"%s\"",
		           catalog.MetastoreName(), error.RawMessage());
	}
}

void IcebergMetastoreTransactionCommit::Commit() {
	if ((!transaction.created_views.empty() && !catalog.SupportsViews(IcebergViewOperation::CREATE)) ||
	    (!transaction.deleted_views.empty() && !catalog.SupportsViews(IcebergViewOperation::LOAD))) {
		transaction.CleanupFiles();
		throw NotImplementedException("Views are not supported by Iceberg %ss", catalog.MetastoreName());
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;
	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!transaction.context.expired()) {
		ClientConfig::GetConfig(*temp_con_context) = ClientConfig::GetConfig(*transaction.context.lock());
	}

	try {
		CommitWithRetries(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		transaction.CleanupFiles();
		temp_con.Rollback();
		transaction.EvictCachedTables();
		error.Throw("Failed to commit Iceberg transaction: ");
	}
	temp_con.Rollback();
}

vector<string> IcebergMetastoreTransactionCommit::SplitNamespace(const string &schema_name) const {
	return StringUtil::Split(schema_name, catalog.namespace_separator);
}

void IcebergMetastoreTransactionCommit::OnEntryChanged(IcebergMetastoreCommit &commit) {
	if (!commit.IsAtomic()) {
		commit.MarkPublished();
	}
}

void IcebergMetastoreTransactionCommit::CommitWithRetries(ClientContext &context) {
	auto alter_update = std::get_if<IcebergTransactionAlterUpdate>(&transaction.transaction_update);
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto commit = catalog.BeginCommit(context);
		written_files.clear();
		on_commit.clear();
		attempted_entries.clear();
		IcebergRetryConfig retry_config;
		bool retryable = false;
		unordered_set<string> table_keys;
		try {
			CreateNamespaces(*commit);
			UpdateNamespaceProperties(*commit);
			std::visit(
			    [&](auto &update) {
				    using T = std::decay_t<decltype(update)>;
				    if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
					    CommitTables(*commit, update, context, retry_config, retryable, table_keys);
				    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
					    DropTable(*commit, update, context);
				    } else if constexpr (std::is_same_v<T, IcebergTransactionRenameUpdate>) {
					    RenameTable(*commit, update);
				    }
			    },
			    transaction.transaction_update);
			DropViews(*commit);
			CreateViews(*commit, context);
			DropNamespaces(*commit);
			FinishAttempt(*commit, context);
		} catch (TransactionException &ex) {
			// A requirement that no longer holds, or a concurrent change to the same entry
			ErrorData error(ex);
			commit->Abort();
			auto published = commit->HasPublished();
			if (published) {
				// The files may be referenced by the published change
				transaction.commit_state_unknown = true;
			} else {
				RemoveWrittenFiles(context);
			}
			if (!alter_update || !retryable || published || attempt >= retry_config.num_retries) {
				error.Throw();
			}
			DUCKDB_LOG(context, IcebergLogType, "Retrying Iceberg %s commit after a conflict: %s",
			           catalog.MetastoreName(), error.RawMessage());
			transaction.RefreshRetryTables(*alter_update, table_keys, context);
			if (!backoff) {
				backoff.emplace(retry_config);
			}
			if (!backoff->WaitBeforeRetry(attempt)) {
				error.Throw();
			}
			continue;
		} catch (...) {
			commit->Abort();
			if (commit->HasPublished()) {
				transaction.commit_state_unknown = true;
			} else {
				RemoveWrittenFiles(context);
			}
			throw;
		}
		for (auto &action : on_commit) {
			action();
		}
		return;
	}
}

void IcebergMetastoreTransactionCommit::RemoveWrittenFiles(ClientContext &context) {
	transaction.CleanupMetadataFiles(context, written_files);
	written_files.clear();
}

void IcebergMetastoreTransactionCommit::CreateNamespaces(IcebergMetastoreCommit &commit) {
	for (auto &created_schema : transaction.created_schemas) {
		auto &schema_name = created_schema.first;
		auto entry = created_schema.second.entry;
		if (commit.NamespaceExists(entry->namespace_items)) {
			transaction.created_schemas.OnNamespaceExists(schema_name);
		} else {
			commit.CreateNamespace(entry->namespace_items);
		}
		on_commit.push_back([this, schema_name, entry]() { catalog.GetSchemas().AddEntry(schema_name, entry); });
	}
}

void IcebergMetastoreTransactionCommit::UpdateNamespaceProperties(IcebergMetastoreCommit &commit) {
	for (auto &properties_update : transaction.schema_property_updates) {
		// Keyed by '<catalog>.<schema>'
		auto schema_name = properties_update.first;
		schema_name.erase(0, schema_name.find('.') + 1);
		auto &updates = properties_update.second;
		commit.UpdateNamespaceProperties(SplitNamespace(schema_name), updates.removals, updates.updates);
	}
}

void IcebergMetastoreTransactionCommit::CommitTables(IcebergMetastoreCommit &commit,
                                                     IcebergTransactionAlterUpdate &alter_update,
                                                     ClientContext &context, IcebergRetryConfig &retry_config,
                                                     bool &retryable, unordered_set<string> &table_keys) {
	auto &fs = FileSystem::GetFileSystem(context);
	bool saw_table = false;
	retryable = true;
	for (auto &entry : alter_update.updated_tables) {
		auto &table_info = entry.second.get();
		if (!table_info.HasTransactionUpdates()) {
			continue;
		}
		// A catalog server creates a table's metadata directory, here the manifests are the first files written to it
		auto table_metadata_directory = table_info.GetMetadata().GetMetadataPath(fs);
		if (!fs.IsRemoteFile(table_metadata_directory)) {
			fs.CreateDirectoriesRecursive(table_metadata_directory);
		}
		auto staged = IcebergCommitStaging::StageSingleTableCommit(db, table_info, context);
		written_files.insert(written_files.end(), staged.created_metadata_files.begin(),
		                     staged.created_metadata_files.end());
		retry_config = saw_table ? retry_config.MostLenient(staged.retry_config) : staged.retry_config;
		retryable = retryable && staged.retryable;
		saw_table = true;
		table_keys.insert(entry.first);

		auto identifier = IcebergMetastoreCatalog::MetastoreIdentifier(table_info.schema, table_info.name);
		const bool creates_table = CreatesTable(staged.request);
		unique_ptr<IcebergTableMetadata> base;
		string base_location;
		if (!creates_table) {
			base_location = commit.LoadMetadataLocation(identifier, IcebergMetastoreEntryType::TABLE);
			if (base_location.empty()) {
				throw TransactionException("Table %s was dropped by a concurrent transaction", identifier.ToString());
			}
			base = make_uniq<IcebergTableMetadata>(
			    IcebergTableMetadata::FromTableMetadata(IcebergTableMetadata::Parse(base_location, fs, "none")));
		}

		auto metadata = IcebergClientCommit::Apply(base.get(), staged.request);
		auto metadata_location =
		    catalog.WriteTableMetadata(context, commit, identifier, metadata, base.get(), base_location);
		if (!commit.HasPublished()) {
			written_files.push_back(metadata_location);
		}

		PublishEntry(commit, context, identifier, IcebergMetastoreEntryType::TABLE, base_location, metadata_location,
		             [&]() {
			             if (creates_table) {
				             commit.CreateEntry(identifier, IcebergMetastoreEntryType::TABLE, metadata_location,
				                                metadata);
			             } else {
				             commit.SwapMetadataLocation(identifier, IcebergMetastoreEntryType::TABLE, base_location,
				                                         metadata_location, metadata);
			             }
		             });
		if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
			on_commit.push_back([this, &table_info]() { catalog.table_request_cache.EvictIfCurrent(table_info); });
		}
	}
	if (!saw_table) {
		retryable = false;
	}
}

void IcebergMetastoreTransactionCommit::DropTable(IcebergMetastoreCommit &commit,
                                                  IcebergTransactionDeleteUpdate &delete_update,
                                                  ClientContext &context) {
	auto &table = delete_update.deleted_table.get();
	auto purge_location = catalog.PurgeLocation(table);
	commit.DropEntry(IcebergMetastoreCatalog::MetastoreIdentifier(table.schema, table.name),
	                 IcebergMetastoreEntryType::TABLE, table.GetMetadata());
	OnEntryChanged(commit);
	on_commit.push_back([this, &table, &context, purge_location]() {
		catalog.table_request_cache.EvictIfCurrent(table);
		DropInfo drop_info;
		drop_info.type = CatalogType::TABLE_ENTRY;
		drop_info.GetQualifiedNameMutable() = Identifier(table.name);
		drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
		table.schema.DropEntry(context, drop_info, true);
		if (!purge_location.empty()) {
			try {
				FileSystem::GetFileSystem(context).RemoveDirectory(purge_location);
			} catch (std::exception &ex) {
				DUCKDB_LOG(context, IcebergLogType, "Could not purge the files of dropped table at '%s': %s",
				           purge_location, ex.what());
			}
		}
	});
}

void IcebergMetastoreTransactionCommit::RenameTable(IcebergMetastoreCommit &commit,
                                                    IcebergTransactionRenameUpdate &rename_update) {
	auto &original_table = rename_update.table.get();
	auto &new_table = rename_update.new_table.get();
	auto new_name = rename_update.new_name;
	commit.RenameEntry(IcebergMetastoreCatalog::MetastoreIdentifier(original_table.schema, original_table.name),
	                   IcebergMetastoreEntryType::TABLE, new_name);
	OnEntryChanged(commit);
	on_commit.push_back([this, &original_table, &new_table, new_name]() {
		if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
			catalog.table_request_cache.EvictIfCurrent(original_table);
			catalog.table_request_cache.EvictIfCurrent(new_table);
		}
		auto &schema = original_table.schema;
		schema.tables.RenameEntry(original_table.name, new_name, std::move(new_table));
	});
}

void IcebergMetastoreTransactionCommit::DropViews(IcebergMetastoreCommit &commit) {
	for (auto &deleted_view : transaction.deleted_views) {
		auto &info = deleted_view.second;
		IcebergMetastoreIdentifier identifier;
		identifier.namespace_items = info.namespace_items;
		identifier.name = info.view_name;
		commit.DropEntry(identifier, IcebergMetastoreEntryType::VIEW, nullptr);
		OnEntryChanged(commit);
	}
	on_commit.push_back([this]() { transaction.deleted_views.clear(); });
}

void IcebergMetastoreTransactionCommit::CreateViews(IcebergMetastoreCommit &commit, ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &created_view : transaction.created_views) {
		auto &view_info = *created_view.second;
		auto &view_name = view_info.GetViewName().GetIdentifierName();
		// A schema created by this transaction is only published once the commit succeeds
		auto &schema_name = view_info.GetQualifiedName().Schema().GetIdentifierName();
		auto pending_schema = transaction.created_schemas.GetEntry(schema_name);
		auto &schema_entry = pending_schema ? *pending_schema
		                                    : catalog.GetSchemas()
		                                          .GetEntry(context, schema_name, OnEntryNotFound::THROW_EXCEPTION)
		                                          ->Cast<IcebergSchemaEntry>();

		// The view's first metadata file, as a catalog server creates it from the CreateViewRequest
		auto request_json = CreateIcebergViewRequestJSON(schema_entry, view_info);
		auto request_doc = JSONDocument::Parse(request_json.c_str(), request_json.size());
		auto request = rest_api_objects::CreateViewRequest::FromJSON(request_doc->GetRoot());
		rest_api_objects::ViewMetadata metadata;
		metadata.view_uuid = UUID::ToString(UUID::GenerateRandomUUID());
		metadata.format_version = 1;
		if (request.location) {
			metadata.location = *request.location;
		} else {
			metadata.location = catalog.DefaultTableLocation(context, schema_entry.namespace_items, view_name);
		}
		StringUtil::RTrim(metadata.location, "/");
		metadata.current_version_id = request.view_version.version_id;
		rest_api_objects::ViewHistoryEntry history;
		history.version_id = request.view_version.version_id;
		history.timestamp_ms = request.view_version.timestamp_ms;
		metadata.version_log.push_back(std::move(history));
		metadata.versions.push_back(request.view_version.Copy());
		metadata.schemas.push_back(request.schema.Copy());
		metadata.properties = request.properties;

		auto metadata_directory = fs.JoinPath(metadata.location, "metadata");
		if (!fs.IsRemoteFile(metadata_directory)) {
			fs.CreateDirectoriesRecursive(metadata_directory);
		}
		auto metadata_location = fs.JoinPath(
		    metadata_directory, StringUtil::Format("00000-%s.metadata.json", UUID::ToString(UUID::GenerateRandomUUID())));
		JSONWriter writer;
		writer.SetRoot(metadata.ToJSON(writer));
		auto content = writer.ToString();
		{
			auto file = fs.OpenFile(metadata_location, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
			file->Write((void *)content.c_str(), content.size());
			file->Close();
		}
		written_files.push_back(metadata_location);

		IcebergMetastoreIdentifier identifier;
		identifier.namespace_items = schema_entry.namespace_items;
		identifier.name = view_name;
		commit.CreateEntry(identifier, IcebergMetastoreEntryType::VIEW, metadata_location, nullptr);
		OnEntryChanged(commit);
	}
	on_commit.push_back([this]() { transaction.created_views.clear(); });
}

void IcebergMetastoreTransactionCommit::DropNamespaces(IcebergMetastoreCommit &commit) {
	for (auto &schema_name : transaction.deleted_schemas) {
		commit.DropNamespace(SplitNamespace(schema_name));
		on_commit.push_back([this, schema_name]() { catalog.GetSchemas().RemoveEntry(schema_name); });
	}
	on_commit.push_back([this]() { transaction.deleted_schemas.clear(); });
}

} // namespace duckdb
