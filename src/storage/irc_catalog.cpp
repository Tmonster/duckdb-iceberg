#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {


IRCatalog::IRCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                     IRCCredentials credentials)
    : Catalog(db_p), access_mode(access_mode), credentials(std::move(credentials)), schemas(*this) {
}

IRCatalog::~IRCatalog() = default;

void IRCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> IRCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropSchema(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void IRCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropSchema(context, info);
}

void IRCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<IRCSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> IRCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                      OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			if (if_not_found == OnEntryNotFound::RETURN_NULL) {
				return nullptr;
			}
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                             "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found, error_context);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}

	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool IRCatalog::InMemory() {
	return false;
}

string IRCatalog::GetDBPath() {
	return internal_name;
}

DatabaseSize IRCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

IRCEndpointBuilder IRCatalog::GetBaseUrl() const {
	auto base_url = IRCEndpointBuilder();
	base_url.SetPrefix(prefix);
	base_url.SetWarehouse(warehouse);
	base_url.SetVersion(version);
	base_url.SetHost(host);
	return base_url;
}

void IRCatalog::ClearCache() {
	schemas.ClearEntries();
}

unique_ptr<SecretEntry> IRCatalog::GetSecret(ClientContext &context, const string &secret_name) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// make sure a secret exists to connect to an AWS catalog
	unique_ptr<SecretEntry> secret_entry = nullptr;
	if (!secret_name.empty()) {
		secret_entry = context.db->GetSecretManager().GetSecretByName(transaction, secret_name);
	}
	if (!secret_entry) {
		auto secret_match = context.db->GetSecretManager().LookupSecret(transaction, "s3://", "s3");
		if (!secret_match.HasMatch()) {
			throw IOException("Failed to find a secret and no explicit secret was passed!");
		}
		secret_entry = std::move(secret_match.secret_entry);
	}
	if (secret_entry) {
		return secret_entry;
	}
	throw IOException("Could not find valid Iceberg secret");
}

unique_ptr<PhysicalOperator> IRCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanInsert");
}
unique_ptr<PhysicalOperator> IRCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                          unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanCreateTableAs");
}
unique_ptr<PhysicalOperator> IRCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanDelete");
}
unique_ptr<PhysicalOperator> IRCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ICCatalog PlanUpdate");
}
unique_ptr<LogicalOperator> IRCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("ICCatalog BindCreateIndex");
}

} // namespace duckdb
