#include "catalog/rest/transaction/iceberg_rest_commit.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/json_document.hpp"

#include <chrono>
#include <optional>
#include <random>
#include <thread>

#include "common/iceberg_constants.hpp"
#include "catalog/rest/iceberg_rest_catalog.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/iceberg_retry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"
#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
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


//! Per-retry-loop backoff state: decorrelated jitter (de-synchronizes a thundering herd of
//! concurrent writers) plus a cumulative total-timeout budget. Independent RNG per loop so
//! concurrent committers do not wake in lockstep.
struct IcebergRetryBackoff {
	explicit IcebergRetryBackoff(const IcebergRetryConfig &config_p)
	    : config(config_p), prev_sleep_ms(config_p.min_wait_ms), start(std::chrono::steady_clock::now()),
	      rng(std::random_device {}() ^
	          static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())),
	      dist(0.0, 1.0) {
	}

	//! Sleep before the next attempt (attempt is the just-failed 0-based attempt index). Returns
	//! false if the wait would exceed commit.retry.total-timeout-ms (only after >=1 retry, mirroring
	//! Java's Tasks.runTaskWithRetry), signalling the caller to stop retrying.
	bool WaitBeforeRetry(idx_t attempt) {
		auto wait_ms = config.DecorrelatedBackoffMs(prev_sleep_ms, dist(rng));
		prev_sleep_ms = wait_ms;
		auto elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		if (attempt > 0 && (wait_ms > config.total_wait_ms || elapsed_ms > config.total_wait_ms - wait_ms)) {
			return false;
		}
		if (wait_ms > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
		}
		return true;
	}

	IcebergRetryConfig config;
	int64_t prev_sleep_ms;
	std::chrono::steady_clock::time_point start;
	std::mt19937_64 rng;
	std::uniform_real_distribution<double> dist;
};


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
		temp_con_context->config = transaction.context.lock()->config;
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
	if (transaction.CanUseMultiTableCommit(alter_update)) {
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

		auto transaction_json = RESTObjectToJSONString(transaction_info.request);
		auto result = IRCAPI::CommitMultiTableUpdate(context, catalog, transaction_json);
		if (result.Success()) {
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
		auto transaction_json = ConstructTableUpdateJSON(table_change);
		auto result =
		    IRCAPI::CommitTableUpdate(context, catalog, identifier._namespace.value, identifier.name, transaction_json);
		if (result.Success()) {
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

		// query may have been moved by ViewCatalogEntry::Initialize(), so use sql field or reconstruct
		string view_sql;
		if (!view_info->sql.empty()) {
			view_sql = view_info->sql;
		} else if (view_info->query) {
			view_sql = view_info->query->ToString();
		} else {
			throw InternalException("Cannot commit view '%s': no SQL representation available", view_name);
		}

		// Build the CreateViewRequest JSON body
		JSONWriter writer;
		auto root_object = writer.CreateObject();
		writer.SetRoot(root_object);

		// name
		root_object.AddString("name", view_name.c_str());

		// schema — the view's output column types
		auto schema_obj = writer.CreateObject();
		root_object.Add("schema", schema_obj);
		schema_obj.AddString("type", "struct");
		schema_obj.Add("schema-id", writer.CreateSignedInteger(0));
		auto fields_arr = writer.CreateArray();
		schema_obj.Add("fields", fields_arr);
		idx_t next_field_id = 1;
		auto column_names = view_info->names;
		for (idx_t i = 0; i < view_info->aliases.size(); i++) {
			if (!view_info->aliases[i].empty()) {
				column_names[i] = view_info->aliases[i];
			}
		}
		// Iceberg schemas require unique field names, just like a bound DuckDB view.
		QueryResult::DeduplicateColumns(column_names);
		for (idx_t i = 0; i < view_info->types.size(); i++) {
			auto &col_name = column_names[i];
			auto field = IcebergTypeHelper::CreateIcebergRestType(
			    col_name.GetIdentifierName(), view_info->types[i], false, "", Value(),
			    [&next_field_id]() { return next_field_id++; }, 2);
			fields_arr.Append(field.ToJSON(writer));
		}
		auto identifier_field_ids = writer.CreateArray();
		schema_obj.Add("identifier-field-ids", identifier_field_ids);
		(void)identifier_field_ids;

		// view-version
		auto version_obj = writer.CreateObject();
		root_object.Add("view-version", version_obj);
		version_obj.Add("version-id", writer.CreateSignedInteger(1));
		version_obj.Add("timestamp-ms",
		                writer.CreateUnsignedInteger(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp())));
		version_obj.Add("schema-id", writer.CreateSignedInteger(0));

		// summary
		auto summary_obj = writer.CreateObject();
		version_obj.Add("summary", summary_obj);
		summary_obj.AddString("engine-name", "DuckDB");

		// default-namespace
		auto ns_arr = writer.CreateArray();
		version_obj.Add("default-namespace", ns_arr);
		for (auto &ns_item : schema_entry.namespace_items) {
			ns_arr.AppendString(ns_item);
		}

		// Omit default-catalog: unqualified references belong to this REST catalog,
		// even when another session attaches it using a different local name.

		// representations
		auto repr_arr = writer.CreateArray();
		version_obj.Add("representations", repr_arr);
		auto repr_obj = writer.CreateObject();
		repr_arr.Append(repr_obj);
		repr_obj.AddString("type", IcebergConstants::ViewSQLRepresentationType);
		repr_obj.AddString("sql", view_sql.c_str());
		repr_obj.AddString("dialect", IcebergConstants::ViewDuckDBDialect);

		// properties (empty)
		root_object.Add("properties", writer.CreateObject());

		auto json_body = writer.ToString();
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
