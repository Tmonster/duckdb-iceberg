#include "catalog/filesystem/iceberg_filesystem_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <optional>

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/filesystem/iceberg_filesystem_catalog.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "catalog/transaction/iceberg_commit_staging.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergFilesystemTransactionCommit::IcebergFilesystemTransactionCommit(IcebergFilesystemCatalog &catalog,
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

//! Create 'path' with 'content', returns false if it exists already
bool WriteNewFile(FileSystem &fs, const string &path, const string &content) {
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	             FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE | FileFlags::FILE_FLAGS_NULL_IF_EXISTS;
	if (fs.FileExists(path)) {
		return false;
	}
	unique_ptr<FileHandle> file;
	try {
		file = fs.OpenFile(path, flags);
	} catch (std::exception &) {
		// Not every file system returns NULL for an existing file
		if (fs.FileExists(path)) {
			return false;
		}
		throw;
	}
	if (!file) {
		return false;
	}
	file->Write((void *)content.c_str(), content.size());
	file->Close();
	return true;
}

void OverwriteFile(FileSystem &fs, const string &path, const string &content) {
	auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	file->Write((void *)content.c_str(), content.size());
	file->Close();
}

} // namespace

void IcebergFilesystemTransactionCommit::Commit() {
	if (!transaction.created_views.empty() || !transaction.deleted_views.empty()) {
		transaction.CleanupFiles();
		throw NotImplementedException("Views are not supported by Iceberg filesystem catalogs");
	}
	if (!transaction.schema_property_updates.empty()) {
		transaction.CleanupFiles();
		throw NotImplementedException("Namespace properties are not supported by Iceberg filesystem catalogs");
	}
	if (std::holds_alternative<IcebergTransactionRenameUpdate>(transaction.transaction_update)) {
		transaction.CleanupFiles();
		throw NotImplementedException("Renaming tables is not supported by Iceberg filesystem catalogs: a table's "
		                              "directory and the paths in its metadata would all have to change");
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;
	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!transaction.context.expired()) {
		temp_con_context->config = transaction.context.lock()->config;
	}

	try {
		CommitWithRetries(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		// Once a change is published, the transaction's data files may be referenced by it
		transaction.commit_state_unknown = published;
		transaction.CleanupFiles();
		temp_con.Rollback();
		transaction.EvictCachedTables();
		error.Throw("Failed to commit Iceberg transaction: ");
	}
	temp_con.Rollback();
}

void IcebergFilesystemTransactionCommit::CommitWithRetries(ClientContext &context) {
	CreateNamespaces(context);
	auto alter_update = std::get_if<IcebergTransactionAlterUpdate>(&transaction.transaction_update);
	if (alter_update) {
		std::optional<IcebergRetryBackoff> backoff;
		for (idx_t attempt = 0;; attempt++) {
			IcebergRetryConfig retry_config;
			bool retryable = false;
			unordered_set<string> table_keys;
			written_files.clear();
			try {
				CommitTables(*alter_update, context, retry_config, retryable, table_keys);
				break;
			} catch (TransactionException &ex) {
				// A requirement that no longer holds, or a concurrent commit of the same version
				ErrorData error(ex);
				transaction.CleanupMetadataFiles(context, written_files);
				if (published || !retryable || attempt >= retry_config.num_retries) {
					error.Throw();
				}
				DUCKDB_LOG(context, IcebergLogType, "Retrying Iceberg filesystem catalog commit after a conflict: %s",
				           error.RawMessage());
				transaction.RefreshRetryTables(*alter_update, table_keys, context);
				if (!backoff) {
					backoff.emplace(retry_config);
				}
				if (!backoff->WaitBeforeRetry(attempt)) {
					error.Throw();
				}
			} catch (...) {
				transaction.CleanupMetadataFiles(context, written_files);
				throw;
			}
		}
	} else if (auto delete_update = std::get_if<IcebergTransactionDeleteUpdate>(&transaction.transaction_update)) {
		DropTable(*delete_update, context);
	}
	DropNamespaces(context);
	for (auto &action : on_commit) {
		action();
	}
}

void IcebergFilesystemTransactionCommit::CreateNamespaces(ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &created_schema : transaction.created_schemas) {
		auto &schema_name = created_schema.first;
		auto entry = created_schema.second.entry;
		auto directory = catalog.NamespaceDirectory(entry->namespace_items);
		if (fs.DirectoryExists(directory)) {
			if (IcebergFilesystemCatalog::IsTableDirectory(fs, directory)) {
				throw CatalogException("Cannot create schema \"%s\": the directory '%s' is a table", schema_name,
				                       directory);
			}
			transaction.created_schemas.OnNamespaceExists(schema_name);
		} else {
			fs.CreateDirectoriesRecursive(directory);
			published = true;
		}
		on_commit.push_back([this, schema_name, entry]() { catalog.GetSchemas().AddEntry(schema_name, entry); });
	}
}

void IcebergFilesystemTransactionCommit::CommitTables(IcebergTransactionAlterUpdate &alter_update,
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
		auto table_directory = catalog.TableDirectory(table_info.schema.namespace_items, table_info.name);
		auto metadata_directory = fs.JoinPath(table_directory, "metadata");
		if (!fs.IsRemoteFile(metadata_directory)) {
			fs.CreateDirectoriesRecursive(metadata_directory);
		}
		auto staged = IcebergCommitStaging::StageSingleTableCommit(db, table_info, context);
		written_files.insert(written_files.end(), staged.created_metadata_files.begin(),
		                     staged.created_metadata_files.end());
		retry_config = saw_table ? retry_config.MostLenient(staged.retry_config) : staged.retry_config;
		retryable = retryable && staged.retryable;
		saw_table = true;
		table_keys.insert(entry.first);

		auto current_version = IcebergFilesystemCatalog::CurrentVersion(fs, table_directory);
		unique_ptr<IcebergTableMetadata> base;
		string base_location;
		idx_t next_version = 1;
		if (CreatesTable(staged.request)) {
			if (current_version.IsValid()) {
				throw CatalogException("Table with name \"%s\" already exists", table_info.name);
			}
		} else {
			if (!current_version.IsValid()) {
				throw TransactionException("Table %s was dropped by a concurrent transaction", table_info.name);
			}
			base_location = IcebergFilesystemCatalog::MetadataFile(fs, table_directory, current_version.GetIndex());
			base = make_uniq<IcebergTableMetadata>(
			    IcebergTableMetadata::FromTableMetadata(IcebergTableMetadata::Parse(base_location, fs, "none")));
			next_version = current_version.GetIndex() + 1;
		}

		auto metadata = IcebergClientCommit::Apply(base.get(), staged.request);
		IcebergClientCommit::RecordPreviousMetadata(metadata, base.get(), base_location);
		auto metadata_location = IcebergFilesystemCatalog::MetadataFile(fs, table_directory, next_version);
		if (!WriteNewFile(fs, metadata_location, metadata.ToJSON())) {
			throw TransactionException("Table %s was changed by a concurrent commit (version %d exists already)",
			                           table_info.name, next_version);
		}
		// The table's new version is committed: the hint only spares readers a search for it
		published = true;
		OverwriteFile(fs, fs.JoinPath(metadata_directory, IcebergFilesystemCatalog::VERSION_HINT_FILE),
		              std::to_string(next_version));

		if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
			on_commit.push_back([this, &table_info]() { catalog.table_request_cache.EvictIfCurrent(table_info); });
		}
	}
	if (!saw_table) {
		retryable = false;
	}
}

void IcebergFilesystemTransactionCommit::DropTable(IcebergTransactionDeleteUpdate &delete_update,
                                                   ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &table = delete_update.deleted_table.get();
	auto table_directory = catalog.TableDirectory(table.schema.namespace_items, table.name);
	if (!IcebergFilesystemCatalog::IsTableDirectory(fs, table_directory)) {
		throw CatalogException("Table with name \"%s\" does not exist", table.name);
	}
	// The table is its directory, so dropping it removes its files. Only files inside the warehouse are removed.
	auto &location = table.table_metadata.GetLocation();
	if (!location.empty() && location != table_directory && !catalog.InWarehouse(location)) {
		throw NotImplementedException("Cannot drop table \"%s\": its location '%s' is outside the warehouse",
		                              table.name, location);
	}
	fs.RemoveDirectory(table_directory);
	published = true;
	on_commit.push_back([this, &table, &context]() {
		catalog.table_request_cache.EvictIfCurrent(table);
		DropInfo drop_info;
		drop_info.type = CatalogType::TABLE_ENTRY;
		drop_info.GetQualifiedNameMutable() = Identifier(table.name);
		drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
		table.schema.DropEntry(context, drop_info, true);
	});
}

void IcebergFilesystemTransactionCommit::DropNamespaces(ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &schema_name : transaction.deleted_schemas) {
		auto directory = catalog.NamespaceDirectory(StringUtil::Split(schema_name, '.'));
		bool empty = true;
		fs.ListFiles(directory, [&](const string &, bool is_directory) {
			if (is_directory) {
				empty = false;
			}
		});
		if (!empty) {
			throw CatalogException("Namespace %s is not empty", schema_name);
		}
		fs.RemoveDirectory(directory);
		published = true;
		on_commit.push_back([this, schema_name]() { catalog.GetSchemas().RemoveEntry(schema_name); });
	}
	on_commit.push_back([this]() { transaction.deleted_schemas.clear(); });
}

} // namespace duckdb
