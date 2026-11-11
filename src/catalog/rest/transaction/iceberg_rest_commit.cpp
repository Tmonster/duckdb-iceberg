#include "catalog/rest/transaction/iceberg_rest_commit.hpp"
#include "catalog/rest/transaction/iceberg_client_commit_verifier.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/json_document.hpp"

#include <optional>

#include "common/iceberg_constants.hpp"
#include "catalog/rest/iceberg_rest_catalog.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/iceberg_view_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/transaction/iceberg_commit_staging.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

IcebergRestTransactionCommit::IcebergRestTransactionCommit(IcebergRestCatalog &catalog, IcebergTransaction &transaction)
    : catalog(catalog), transaction(transaction), db(transaction.db) {
}

namespace {

template <class RESTObject>
string RESTObjectToJSONString(const RESTObject &object) {
	JSONWriter writer;
	writer.SetRoot(object.ToJSON(writer));
	return writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN);
}

string ConstructTableUpdateJSON(rest_api_objects::CommitTableRequest &table_change) {
	return RESTObjectToJSONString(table_change);
}


bool CommitIsRetryable(bool retryable, idx_t max_retries, const CommitResult &result, idx_t attempt) {
	if (attempt >= max_retries) {
		//! We've reached the max amount of retries
		return false;
	}
	if (!retryable) {
		//! The operation isn't retryable in general
		return false;
	}
	if (!result.IsConflict()) {
		//! Only conflicts (409) are retryable
		return false;
	}
	return true;
}

// 4xx = definitive rejection (commit did not land) -> delete the orphan. 5xx / no HTTP status
// (connection error/timeout) = outcome unknown (may have landed) -> keep files. Status is in
// extra_info (HTTPException) or the message "status code (<status>)" (fallback).
bool CommitStateUnknown(const ErrorData &error) {
	auto &extra = error.ExtraInfo();
	auto it = extra.find("status_code");
	if (it != extra.end()) {
		return it->second.empty() || it->second[0] != '4';
	}
	auto &msg = error.RawMessage();
	auto pos = msg.find("status code (");
	if (pos != string::npos) {
		auto digit = msg.find_first_of("0123456789", pos);
		if (digit != string::npos) {
			return msg[digit] != '4';
		}
	}
	return true;
}

} // namespace

void IcebergRestTransactionCommit::Commit() {
	// Views use individual REST endpoints, not the atomic table commit endpoint. Validate
	// the entire write set before any schema, table, or view request can reach the catalog.
	auto view_operations = transaction.created_views.size() + transaction.deleted_views.size();
	if (view_operations && (view_operations > 1 || transaction.HasTableUpdate() || !transaction.created_schemas.Empty() ||
	                        !transaction.deleted_schemas.empty() || !transaction.schema_property_updates.empty())) {
		transaction.CleanupFiles();
		throw TransactionException("Iceberg REST Catalog cannot commit view changes atomically with other catalog "
		                           "changes; commit each view create or drop separately");
	}

	Connection temp_con(db);
	temp_con.BeginTransaction();
	auto &temp_con_context = temp_con.context;

	// Copy user settings from the original context so that e.g. s3_access_key_id are available
	if (!transaction.context.expired()) {
		ClientConfig::GetConfig(*temp_con_context) = ClientConfig::GetConfig(*transaction.context.lock());
	}

	try {
		DoSchemaCreates(*temp_con_context);
		DoSchemaPropertyUpdates(*temp_con_context);
		std::visit(
		    [&](auto &update) {
			    using T = std::decay_t<decltype(update)>;
			    if constexpr (std::is_same_v<T, std::monostate>) {
				    return;
			    } else if constexpr (std::is_same_v<T, IcebergTransactionAlterUpdate>) {
				    DoTableUpdates(update, *temp_con_context);
			    } else if constexpr (std::is_same_v<T, IcebergTransactionDeleteUpdate>) {
				    DoTableDeletes(update, *temp_con_context);
			    } else if constexpr (std::is_same_v<T, IcebergTransactionRenameUpdate>) {
				    DoTableRename(update, *temp_con_context);
			    }
		    },
		    transaction.transaction_update);
		DoViewDeletes(*temp_con_context);
		DoViewCreates(*temp_con_context);
		DoSchemaDeletes(*temp_con_context);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		transaction.commit_state_unknown = CommitStateUnknown(error);
		transaction.CleanupFiles();
		temp_con.Rollback();
		transaction.EvictCachedTables();
		error.Throw("Failed to commit Iceberg transaction: ");
	}

	temp_con.Rollback();
}

void IcebergRestTransactionCommit::DoTableUpdates(IcebergTransactionAlterUpdate &alter_update, ClientContext &context) {
	if (!alter_update.HasUpdates()) {
		return;
	}
	if (catalog.CanUseMultiTableCommit(alter_update)) {
		DoMultiTableCommitUpdates(alter_update, context);
	} else {
		DoSingleTableCommitUpdates(alter_update, context);
	}

	auto &ic_catalog = catalog;
	if (ic_catalog.attach_options.max_table_staleness_micros.IsValid()) {
		for (auto &entry : alter_update.updated_tables) {
			ic_catalog.table_request_cache.EvictIfCurrent(entry.second.get());
		}
	}
}

void IcebergRestTransactionCommit::DoTableRename(IcebergTransactionRenameUpdate &rename_update, ClientContext &context) {
	auto &original_table = rename_update.table.get();
	auto &schema = original_table.schema;
	auto &table_name = original_table.name;
	auto new_name = rename_update.new_name;
	auto &new_table = rename_update.new_table.get();

	rest_api_objects::RenameTableRequest request;
	request.source._namespace.value = schema.namespace_items;
	request.source.name = table_name;
	request.destination._namespace.value = schema.namespace_items;
	request.destination.name = new_name;
	auto transaction_json = RESTObjectToJSONString(request);
	IRCAPI::CommitTableRename(context, catalog, transaction_json);

	if (catalog.attach_options.max_table_staleness_micros.IsValid()) {
		//! The shared cache must only change once the catalog rename is durable.
		catalog.table_request_cache.EvictIfCurrent(original_table);
		catalog.table_request_cache.EvictIfCurrent(new_table);
	}

	schema.tables.RenameEntry(table_name, new_name, std::move(new_table));
}

void IcebergRestTransactionCommit::DoMultiTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                   ClientContext &context) {
	//! The retry policy is the tables' folded (most-lenient) config, produced by StageMultiTableCommit.
	//! It is stable across attempts (same tables), so the backoff state is created once, lazily, from
	//! the first staged request and reused for every retry.
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto transaction_info = IcebergCommitStaging::StageMultiTableCommit(db, alter_update, context);
		if (transaction_info.request.table_changes.empty()) {
			alter_update.updated_tables.clear();
			return;
		}
		if (!backoff) {
			backoff.emplace(transaction_info.retry_config);
		}

		unique_ptr<IcebergClientCommitVerifier> verifier;
		if (IcebergClientCommitVerifier::IsEnabled(context)) {
			verifier = make_uniq<IcebergClientCommitVerifier>(context, catalog);
			for (auto &table_change : transaction_info.request.table_changes) {
				verifier->Prepare(table_change);
			}
		}
		auto transaction_json = RESTObjectToJSONString(transaction_info.request);
		auto result = IRCAPI::CommitMultiTableUpdate(context, catalog, transaction_json);
		if (result.Success()) {
			if (verifier) {
				verifier->Verify(nullptr);
			}
			return;
		}
		if (!CommitIsRetryable(transaction_info.retryable, transaction_info.retry_config.num_retries, result,
		                       attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
		transaction.CleanupMetadataFiles(context, transaction_info.created_metadata_files);
		transaction.RefreshRetryTables(alter_update, transaction_info.table_keys, context);
		//! Back off before the next attempt to de-synchronize concurrent writers; stop if the
		//! cumulative retry budget (commit.retry.total-timeout-ms) would be exceeded.
		if (!backoff->WaitBeforeRetry(attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
	}
}

void IcebergRestTransactionCommit::DoSingleTableCommitUpdates(IcebergTransactionAlterUpdate &alter_update,
                                                    ClientContext &context) {
	D_ASSERT(catalog.supported_urls.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
	auto table_info = IcebergCommitStaging::GetSingleUpdatedTable(alter_update);
	if (!table_info) {
		return;
	}
	auto table_key = table_info->GetTableKey();
	//! Backoff state is created once (lazily, from the first staged commit's config) and reused
	//! across retries for this single atomic request.
	std::optional<IcebergRetryBackoff> backoff;
	for (idx_t attempt = 0;; attempt++) {
		auto table_transaction_info = IcebergCommitStaging::StageSingleTableCommit(db, *table_info, context);
		if (!backoff) {
			backoff.emplace(table_transaction_info.retry_config);
		}
		auto &table_change = table_transaction_info.request;
		D_ASSERT(table_change.identifier);
		auto &identifier = *table_change.identifier;
		unique_ptr<IcebergClientCommitVerifier> verifier;
		if (IcebergClientCommitVerifier::IsEnabled(context)) {
			verifier = make_uniq<IcebergClientCommitVerifier>(context, catalog);
			verifier->Prepare(table_change);
		}
		auto transaction_json = ConstructTableUpdateJSON(table_change);
		auto result =
		    IRCAPI::CommitTableUpdate(context, catalog, identifier._namespace.value, identifier.name, transaction_json);
		if (result.Success()) {
			if (verifier) {
				verifier->Verify(&result.body);
			}
			return;
		}

		if (!CommitIsRetryable(table_transaction_info.retryable, table_transaction_info.retry_config.num_retries,
		                       result, attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
		transaction.CleanupMetadataFiles(context, table_transaction_info.created_metadata_files);
		unordered_set<string> retry_tables;
		retry_tables.insert(table_key);
		transaction.RefreshRetryTables(alter_update, retry_tables, context);
		//! Back off before the next attempt; stop if the retry budget would be exceeded.
		if (!backoff->WaitBeforeRetry(attempt)) {
			result.Throw(catalog.GetBaseUrl().GetURLEncoded());
		}
	}
}

void IcebergRestTransactionCommit::DoTableDeletes(IcebergTransactionDeleteUpdate &delete_update, ClientContext &context) {
	auto &ic_catalog = catalog;
	auto &table = delete_update.deleted_table.get();
	auto &table_name = table.name;
	IRCAPI::CommitTableDelete(context, catalog, table.schema.namespace_items, table_name);
	// remove the load table result
	ic_catalog.table_request_cache.EvictIfCurrent(table);
	// remove the table entry from the catalog
	DropInfo drop_info;
	drop_info.type = CatalogType::TABLE_ENTRY;
	drop_info.GetQualifiedNameMutable() = Identifier(table_name);
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	table.schema.DropEntry(context, drop_info, true);
}

void IcebergRestTransactionCommit::DoSchemaCreates(ClientContext &context) {
	auto &ic_catalog = catalog;
	for (auto &created_schema : transaction.created_schemas) {
		auto &schema_name = created_schema.first;
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name);

		rest_api_objects::CreateNamespaceRequest request;
		request._namespace.value = namespace_identifiers;
		request.properties = case_insensitive_map_t<string>();
		auto create_body = RESTObjectToJSONString(request);

		if (!IRCAPI::CommitNamespaceCreate(context, ic_catalog, schema_name, create_body)) {
			transaction.created_schemas.OnNamespaceExists(schema_name);
		}
		ic_catalog.GetSchemas().AddEntry(schema_name, created_schema.second.entry);
	}
}

void IcebergRestTransactionCommit::DoSchemaDeletes(ClientContext &context) {
	auto &ic_catalog = catalog;
	for (auto &schema_name : transaction.deleted_schemas) {
		vector<string> namespace_items;
		auto namespace_identifier = IRCAPI::ParseSchemaName(schema_name);
		IRCAPI::CommitNamespaceDrop(context, ic_catalog, namespace_identifier);
		ic_catalog.GetSchemas().RemoveEntry(schema_name);
	}
	transaction.deleted_schemas.clear();
}

void IcebergRestTransactionCommit::DoSchemaPropertyUpdates(ClientContext &context) {
	auto &ic_catalog = catalog;
	for (auto &properties_update : transaction.schema_property_updates) {
		auto schema_name_with_catalog = properties_update.first;
		auto catalog_splitter = schema_name_with_catalog.find(".");
		auto schema_name_no_catalog = schema_name_with_catalog.erase(0, catalog_splitter + 1);

		auto schema_property_updates = properties_update.second;
		auto namespace_identifiers = IRCAPI::ParseSchemaName(schema_name_no_catalog);

		rest_api_objects::UpdateNamespacePropertiesRequest request;
		request.removals = vector<string>();
		request.removals->assign(schema_property_updates.removals.begin(), schema_property_updates.removals.end());
		request.updates = schema_property_updates.updates;
		auto create_body = RESTObjectToJSONString(request);

		IRCAPI::CommitNamespacePropertiesUpdate(context, ic_catalog, create_body, namespace_identifiers);
	}
}

void IcebergRestTransactionCommit::DoViewCreates(ClientContext &context) {
	if (transaction.created_views.empty()) {
		return;
	}
	auto &ic_catalog = catalog;
	for (auto &view_entry : transaction.created_views) {
		auto &view_info = view_entry.second;
		auto &view_name = view_info->GetViewName().GetIdentifierName();
		// Look up the schema entry from the catalog by name
		auto &schema_entry = ic_catalog.schemas
		                         .GetEntry(context, view_info->GetQualifiedName().Schema().GetIdentifierName(),
		                                   OnEntryNotFound::THROW_EXCEPTION)
		                         ->Cast<IcebergSchemaEntry>();

		auto json_body = CreateIcebergViewRequestJSON(schema_entry, *view_info);
		IRCAPI::CommitNewView(context, catalog, schema_entry, json_body);
	}
	transaction.created_views.clear();
}

void IcebergRestTransactionCommit::DoViewDeletes(ClientContext &context) {
	for (auto &deleted_view : transaction.deleted_views) {
		auto &info = deleted_view.second;
		IRCAPI::CommitViewDelete(context, catalog, info.namespace_items, info.view_name);
	}
	transaction.deleted_views.clear();
}

} // namespace duckdb
