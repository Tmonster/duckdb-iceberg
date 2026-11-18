#include "catalog/metastore/iceberg_metastore_catalog.hpp"

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/metastore/iceberg_metastore_commit.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

string IcebergMetastoreIdentifier::ToString() const {
	auto result = StringUtil::Join(namespace_items, ".");
	return result.empty() ? name : result + "." + name;
}

void IcebergMetastoreCommit::UpdateNamespaceProperties(const vector<string> &namespace_items,
                                                       const set<string> &removals,
                                                       const case_insensitive_map_t<string> &updates) {
	throw NotImplementedException("Namespace properties are not supported by this Iceberg catalog");
}

void IcebergMetastoreCommit::RenameEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
                                         const string &new_name) {
	throw NotImplementedException("Renaming tables is not supported by this Iceberg catalog");
}

IcebergMetastoreCatalog::IcebergMetastoreCatalog(IcebergCatalogType catalog_type, AttachedDatabase &db_p,
                                                 AccessMode access_mode, IcebergAttachOptions &attach_options_p,
                                                 const optional<duckdb::Identifier> &default_schema)
    : IcebergCatalog(catalog_type, db_p, access_mode, attach_options_p, default_schema) {
}

IcebergMetastoreCatalog::~IcebergMetastoreCatalog() = default;

IcebergMetastoreIdentifier IcebergMetastoreCatalog::MetastoreIdentifier(const IcebergSchemaEntry &schema,
                                                                       const string &name) {
	IcebergMetastoreIdentifier result;
	result.namespace_items = schema.namespace_items;
	result.name = name;
	return result;
}

bool IcebergMetastoreCatalog::InWarehouse(const string &location) const {
	auto warehouse_root = GetWarehouse();
	StringUtil::RTrim(warehouse_root, "/");
	return !warehouse_root.empty() && StringUtil::StartsWith(location, warehouse_root + "/");
}

IcebergLoadTableResult IcebergMetastoreCatalog::LoadTable(ClientContext &context,
                                                          const vector<string> &namespace_items,
                                                          const string &table_name) {
	IcebergMetastoreIdentifier identifier;
	identifier.namespace_items = namespace_items;
	identifier.name = table_name;
	auto metadata_location = GetMetadataLocation(context, identifier, IcebergMetastoreEntryType::TABLE);
	IcebergLoadTableResult result;
	if (metadata_location.empty()) {
		result.status_ = HTTPStatusCode::NotFound_404;
		rest_api_objects::IcebergErrorResponse error;
		error._error.type = "NoSuchTableException";
		error._error.message = StringUtil::Format("Table does not exist: %s", identifier.ToString());
		error._error.code = 404;
		result.error_ = std::move(error);
		return result;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto load_table_result = make_uniq<rest_api_objects::LoadTableResult>();
	load_table_result->metadata = IcebergTableMetadata::Parse(metadata_location, fs, "none");
	load_table_result->metadata_location = metadata_location;
	result.status_ = HTTPStatusCode::OK_200;
	result.result_ = make_shared_ptr<const IcebergLoadTableResponse>(std::move(*load_table_result));
	return result;
}

IcebergTable &IcebergMetastoreCatalog::CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
                                                   const IcebergCreateTableRequest &request) {
	auto location = request.GetLocation();
	if (location.empty()) {
		location = DefaultTableLocation(context, schema.namespace_items, request.GetName());
	}
	StringUtil::RTrim(location, "/");
	auto metadata = request.CreateInitialMetadata(UUID::ToString(UUID::GenerateRandomUUID()), location);
	auto &table_info = AddCreatedTable(context, IcebergTable(*this, schema, request.GetName(), std::move(metadata)));
	// The table is created from empty metadata at commit, like a table staged in a REST catalog
	auto &iceberg_transaction = IcebergTransaction::Get(context, *this);
	auto &transaction_data = table_info.GetOrCreateTransactionData(iceberg_transaction);
	transaction_data.TableAddAssertCreate();
	AddCreateTableUpdates(context, table_info);
	return table_info;
}

string IcebergMetastoreCatalog::WriteTableMetadata(ClientContext &context, IcebergMetastoreCommit &commit,
                                                   const IcebergMetastoreIdentifier &identifier,
                                                   IcebergTableMetadata &metadata,
                                                   optional_ptr<const IcebergTableMetadata> base,
                                                   const string &base_location) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto metadata_directory = metadata.GetMetadataPath(fs);
	if (!fs.IsRemoteFile(metadata_directory)) {
		fs.CreateDirectoriesRecursive(metadata_directory);
	}
	return IcebergClientCommit::WriteMetadataFile(context, metadata, base, base_location, metadata_directory);
}

string IcebergMetastoreCatalog::PurgeLocation(const IcebergTable &table) const {
	if (!attach_options.purge_requested) {
		return string();
	}
	// Only remove files that belong to the catalog's warehouse
	auto &location = table.GetMetadata().GetLocation();
	return InWarehouse(location) ? location : string();
}

void IcebergMetastoreCatalog::VerifySingleTableUpdate(const IcebergTransactionAlterUpdate &alter_update) const {
	if (alter_update.updated_tables.size() > 1) {
		throw TransactionException("Iceberg %s cannot commit this transaction atomically because it would change "
		                           "multiple tables, and every table is committed separately",
		                           MetastoreName());
	}
}

void IcebergMetastoreCatalog::Commit(IcebergTransaction &transaction) {
	IcebergMetastoreTransactionCommit(*this, transaction).Commit();
}

} // namespace duckdb
