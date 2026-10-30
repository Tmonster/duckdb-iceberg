#include "catalog/sql/iceberg_sql_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
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
#include "catalog/sql/iceberg_sql_catalog.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "catalog/transaction/iceberg_commit_staging.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

IcebergSqlTransactionCommit::IcebergSqlTransactionCommit(IcebergSqlCatalog &catalog, IcebergTransaction &transaction)
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

idx_t ChangedRows(const vector<vector<Value>> &result) {
	if (result.empty() || result[0].empty()) {
		return 0;
	}
	return result[0][0].GetValue<int64_t>();
}

void RollbackQuietly(Connection &connection) {
	if (connection.HasActiveTransaction()) {
		(void)connection.Query("ROLLBACK");
	}
}

} // namespace

void IcebergSqlTransactionCommit::Commit() {
	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;
	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!transaction.context.expired()) {
		temp_con_context->config = transaction.context.lock()->config;
	}

	try {
		catalog.Initialize(*temp_con_context);
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

void IcebergSqlTransactionCommit::CommitWithRetries(ClientContext &context) {
	auto alter_update = std::get_if<IcebergTransactionAlterUpdate>(&transaction.transaction_update);
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto connection = catalog.Connect();
		connection->BeginTransaction();
		written_files.clear();
		on_commit.clear();
		IcebergRetryConfig retry_config;
		bool retryable = false;
		unordered_set<string> table_keys;
		try {
			WriteSchemaCreates(*connection);
			WriteSchemaPropertyUpdates(*connection);
			std::visit(
			    [&](auto &update) {
				    using T = std::decay_t<decltype(update)>;
				    if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
					    WriteTableUpdates(*connection, update, context, retry_config, retryable, table_keys);
				    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
					    WriteTableDelete(*connection, update, context);
				    } else if constexpr (std::is_same_v<T, IcebergTransactionRenameUpdate>) {
					    WriteTableRename(*connection, update, context);
				    }
			    },
			    transaction.transaction_update);
			WriteViewDeletes(*connection);
			WriteViewCreates(*connection, context);
			WriteSchemaDeletes(*connection);
			connection->Commit();
		} catch (TransactionException &ex) {
			// A requirement that no longer holds, or a concurrent change to the same catalog rows
			ErrorData error(ex);
			RollbackQuietly(*connection);
			RemoveWrittenFiles(context);
			if (!alter_update || !retryable || attempt >= retry_config.num_retries) {
				error.Throw();
			}
			DUCKDB_LOG(context, IcebergLogType, "Retrying Iceberg SQL catalog commit after a conflict: %s",
			           error.RawMessage());
			transaction.RefreshRetryTables(*alter_update, table_keys, context);
			if (!backoff) {
				backoff.emplace(retry_config);
			}
			if (!backoff->WaitBeforeRetry(attempt)) {
				error.Throw();
			}
			continue;
		} catch (...) {
			RollbackQuietly(*connection);
			RemoveWrittenFiles(context);
			throw;
		}
		for (auto &action : on_commit) {
			action();
		}
		return;
	}
}

void IcebergSqlTransactionCommit::RemoveWrittenFiles(ClientContext &context) {
	transaction.CleanupMetadataFiles(context, written_files);
	written_files.clear();
}

void IcebergSqlTransactionCommit::WriteSchemaCreates(Connection &connection) {
	auto &catalog_name = catalog.GetCatalogName();
	for (auto &created_schema : transaction.created_schemas) {
		auto &schema_name = created_schema.first;
		auto existing = IcebergSqlCatalog::Query(
		    connection,
		    StringUtil::Format("SELECT 1 FROM %s WHERE catalog_name = $1 AND namespace = $2 "
		                       "UNION ALL SELECT 1 FROM %s WHERE catalog_name = $1 AND table_namespace = $2 LIMIT 1",
		                       catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE),
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
		    {Value(catalog_name), Value(schema_name)});
		if (!existing.empty()) {
			transaction.created_schemas.OnNamespaceExists(schema_name);
		} else {
			// The JDBC catalog records namespaces without other properties with 'exists' = 'true'
			IcebergSqlCatalog::Query(
			    connection,
			    StringUtil::Format("INSERT INTO %s (catalog_name, namespace, property_key, property_value) "
			                       "VALUES ($1, $2, 'exists', 'true')",
			                       catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE)),
			    {Value(catalog_name), Value(schema_name)});
		}
		auto entry = created_schema.second.entry;
		on_commit.push_back([this, schema_name, entry]() { catalog.GetSchemas().AddEntry(schema_name, entry); });
	}
}

void IcebergSqlTransactionCommit::WriteSchemaPropertyUpdates(Connection &connection) {
	auto &catalog_name = catalog.GetCatalogName();
	auto properties_table = catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE);
	for (auto &properties_update : transaction.schema_property_updates) {
		// Keyed by '<catalog>.<schema>'
		auto schema_name = properties_update.first;
		schema_name.erase(0, schema_name.find('.') + 1);
		auto &updates = properties_update.second;
		auto delete_property = StringUtil::Format(
		    "DELETE FROM %s WHERE catalog_name = $1 AND namespace = $2 AND property_key = $3", properties_table);
		for (auto &key : updates.removals) {
			IcebergSqlCatalog::Query(connection, delete_property,
			                         {Value(catalog_name), Value(schema_name), Value(key)});
		}
		for (auto &property : updates.updates) {
			IcebergSqlCatalog::Query(connection, delete_property,
			                         {Value(catalog_name), Value(schema_name), Value(property.first)});
			IcebergSqlCatalog::Query(
			    connection,
			    StringUtil::Format("INSERT INTO %s (catalog_name, namespace, property_key, property_value) "
			                       "VALUES ($1, $2, $3, $4)",
			                       properties_table),
			    {Value(catalog_name), Value(schema_name), Value(property.first), Value(property.second)});
		}
	}
}

void IcebergSqlTransactionCommit::WriteTableUpdates(Connection &connection,
                                                    IcebergTransactionAlterUpdate &alter_update,
                                                    ClientContext &context, IcebergRetryConfig &retry_config,
                                                    bool &retryable, unordered_set<string> &table_keys) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &catalog_name = catalog.GetCatalogName();
	auto tables_table = catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE);
	bool saw_table = false;
	retryable = true;
	for (auto &entry : alter_update.updated_tables) {
		auto &table_info = entry.second.get();
		if (!table_info.HasTransactionUpdates()) {
			continue;
		}
		// A catalog server creates a table's metadata directory, here the manifests are the first files written to it
		auto table_metadata_directory = table_info.table_metadata.GetMetadataPath(fs);
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

		auto namespace_name = IcebergSqlCatalog::NamespaceName(table_info.schema.namespace_items);
		auto &table_name = table_info.name;
		const bool creates_table = CreatesTable(staged.request);
		unique_ptr<IcebergTableMetadata> base;
		string base_location;
		if (!creates_table) {
			auto rows = IcebergSqlCatalog::Query(
			    connection,
			    StringUtil::Format("SELECT metadata_location FROM %s "
			                       "WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3",
			                       tables_table),
			    {Value(catalog_name), Value(namespace_name), Value(table_name)});
			if (rows.empty() || rows[0][0].IsNull()) {
				throw TransactionException("Table %s.%s was dropped by a concurrent transaction", namespace_name,
				                           table_name);
			}
			base_location = rows[0][0].ToString();
			base = make_uniq<IcebergTableMetadata>(
			    IcebergTableMetadata::FromTableMetadata(IcebergTableMetadata::Parse(base_location, fs, "none")));
		}

		auto metadata = IcebergClientCommit::Apply(base.get(), staged.request);
		auto metadata_directory = metadata.GetMetadataPath(fs);
		if (!fs.IsRemoteFile(metadata_directory)) {
			fs.CreateDirectoriesRecursive(metadata_directory);
		}
		auto metadata_location =
		    IcebergClientCommit::WriteMetadataFile(context, metadata, base.get(), base_location, metadata_directory);
		written_files.push_back(metadata_location);

		if (creates_table) {
			try {
				IcebergSqlCatalog::Query(
				    connection,
				    StringUtil::Format("INSERT INTO %s (catalog_name, table_namespace, table_name, metadata_location, "
				                       "previous_metadata_location%s) VALUES ($1, $2, $3, $4, NULL%s)",
				                       tables_table, catalog.HasIcebergTypeColumn() ? ", iceberg_type" : "",
				                       catalog.HasIcebergTypeColumn() ? ", 'TABLE'" : ""),
				    {Value(catalog_name), Value(namespace_name), Value(table_name), Value(metadata_location)});
			} catch (ConstraintException &) {
				throw CatalogException("Table with name \"%s\" already exists", table_name);
			}
		} else {
			auto changed = ChangedRows(IcebergSqlCatalog::Query(
			    connection,
			    StringUtil::Format("UPDATE %s SET metadata_location = $1, previous_metadata_location = $2 "
			                       "WHERE catalog_name = $3 AND table_namespace = $4 AND table_name = $5 "
			                       "AND metadata_location = $2",
			                       tables_table),
			    {Value(metadata_location), Value(base_location), Value(catalog_name), Value(namespace_name),
			     Value(table_name)}));
			if (changed != 1) {
				throw TransactionException("Table %s.%s was changed by a concurrent transaction", namespace_name,
				                           table_name);
			}
		}
		if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
			on_commit.push_back([this, &table_info]() { catalog.table_request_cache.EvictIfCurrent(table_info); });
		}
	}
	if (!saw_table) {
		retryable = false;
	}
}

void IcebergSqlTransactionCommit::WriteTableDelete(Connection &connection,
                                                   IcebergTransactionDeleteUpdate &delete_update,
                                                   ClientContext &context) {
	auto &table = delete_update.deleted_table.get();
	auto namespace_name = IcebergSqlCatalog::NamespaceName(table.schema.namespace_items);
	auto changed = ChangedRows(IcebergSqlCatalog::Query(
	    connection,
	    StringUtil::Format("DELETE FROM %s WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3",
	                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
	    {Value(catalog.GetCatalogName()), Value(namespace_name), Value(table.name)}));
	if (changed != 1) {
		throw CatalogException("Table with name \"%s\" does not exist", table.name);
	}
	string purge_location;
	if (catalog.attach_options.purge_requested) {
		// Only remove files that belong to the catalog's warehouse
		auto warehouse_root = catalog.GetWarehouse();
		StringUtil::RTrim(warehouse_root, "/");
		auto &location = table.table_metadata.GetLocation();
		if (!warehouse_root.empty() && StringUtil::StartsWith(location, warehouse_root + "/")) {
			purge_location = location;
		}
	}
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

void IcebergSqlTransactionCommit::WriteTableRename(Connection &connection,
                                                   IcebergTransactionRenameUpdate &rename_update,
                                                   ClientContext &context) {
	auto &original_table = rename_update.table.get();
	auto &new_table = rename_update.new_table.get();
	auto new_name = rename_update.new_name;
	auto namespace_name = IcebergSqlCatalog::NamespaceName(original_table.schema.namespace_items);
	idx_t changed;
	try {
		changed = ChangedRows(IcebergSqlCatalog::Query(
		    connection,
		    StringUtil::Format("UPDATE %s SET table_name = $1 "
		                       "WHERE catalog_name = $2 AND table_namespace = $3 AND table_name = $4",
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
		    {Value(new_name), Value(catalog.GetCatalogName()), Value(namespace_name), Value(original_table.name)}));
	} catch (ConstraintException &) {
		throw CatalogException("Table with name \"%s\" already exists", new_name);
	}
	if (changed != 1) {
		throw CatalogException("Table with name \"%s\" does not exist", original_table.name);
	}
	on_commit.push_back([this, &original_table, &new_table, new_name]() {
		if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
			catalog.table_request_cache.EvictIfCurrent(original_table);
			catalog.table_request_cache.EvictIfCurrent(new_table);
		}
		auto &schema = original_table.schema;
		schema.tables.RenameEntry(original_table.name, new_name, std::move(new_table));
	});
}

void IcebergSqlTransactionCommit::WriteViewDeletes(Connection &connection) {
	for (auto &deleted_view : transaction.deleted_views) {
		auto &info = deleted_view.second;
		auto changed = ChangedRows(IcebergSqlCatalog::Query(
		    connection,
		    StringUtil::Format("DELETE FROM %s WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3 "
		                       "AND iceberg_type = 'VIEW'",
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
		    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(info.namespace_items)),
		     Value(info.view_name)}));
		if (changed != 1) {
			throw CatalogException("View with name \"%s\" does not exist", info.view_name);
		}
	}
	on_commit.push_back([this]() { transaction.deleted_views.clear(); });
}

void IcebergSqlTransactionCommit::WriteViewCreates(Connection &connection, ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &created_view : transaction.created_views) {
		auto &view_info = *created_view.second;
		auto &view_name = view_info.GetViewName().GetIdentifierName();
		// A schema created by this transaction is only published once the catalog transaction commits
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

		try {
			IcebergSqlCatalog::Query(
			    connection,
			    StringUtil::Format("INSERT INTO %s (catalog_name, table_namespace, table_name, metadata_location, "
			                       "previous_metadata_location, iceberg_type) VALUES ($1, $2, $3, $4, NULL, 'VIEW')",
			                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
			    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(schema_entry.namespace_items)),
			     Value(view_name), Value(metadata_location)});
		} catch (ConstraintException &) {
			throw CatalogException("A table or view with name \"%s\" already exists", view_name);
		}
	}
	on_commit.push_back([this]() { transaction.created_views.clear(); });
}

void IcebergSqlTransactionCommit::WriteSchemaDeletes(Connection &connection) {
	auto &catalog_name = catalog.GetCatalogName();
	for (auto &schema_name : transaction.deleted_schemas) {
		auto tables = IcebergSqlCatalog::Query(
		    connection,
		    StringUtil::Format("SELECT 1 FROM %s WHERE catalog_name = $1 AND table_namespace = $2 LIMIT 1",
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
		    {Value(catalog_name), Value(schema_name)});
		if (!tables.empty()) {
			throw CatalogException("Namespace %s is not empty", schema_name);
		}
		IcebergSqlCatalog::Query(
		    connection,
		    StringUtil::Format("DELETE FROM %s WHERE catalog_name = $1 AND namespace = $2",
		                       catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE)),
		    {Value(catalog_name), Value(schema_name)});
		on_commit.push_back([this, schema_name]() { catalog.GetSchemas().RemoveEntry(schema_name); });
	}
	on_commit.push_back([this]() { transaction.deleted_schemas.clear(); });
}

} // namespace duckdb
