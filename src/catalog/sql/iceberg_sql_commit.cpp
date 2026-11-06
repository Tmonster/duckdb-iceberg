#include "catalog/sql/iceberg_sql_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/sql/iceberg_sql_catalog.hpp"

namespace duckdb {

namespace {

idx_t ChangedRows(const vector<vector<Value>> &result) {
	if (result.empty() || result[0].empty()) {
		return 0;
	}
	return result[0][0].GetValue<int64_t>();
}

const char *TypeName(IcebergMetastoreEntryType type) {
	return type == IcebergMetastoreEntryType::VIEW ? "VIEW" : "TABLE";
}

const char *EntryKind(IcebergMetastoreEntryType type) {
	return type == IcebergMetastoreEntryType::VIEW ? "View" : "Table";
}

} // namespace

IcebergSqlMetastoreCommit::IcebergSqlMetastoreCommit(IcebergSqlCatalog &catalog, unique_ptr<Connection> connection_p)
    : catalog(catalog), connection(std::move(connection_p)) {
	connection->BeginTransaction();
}

IcebergSqlMetastoreCommit::~IcebergSqlMetastoreCommit() {
	Abort();
}

string IcebergSqlMetastoreCommit::TypeCondition(IcebergMetastoreEntryType type) const {
	if (!catalog.HasIcebergTypeColumn()) {
		return string();
	}
	if (type == IcebergMetastoreEntryType::VIEW) {
		return " AND iceberg_type = 'VIEW'";
	}
	return " AND coalesce(iceberg_type, 'TABLE') = 'TABLE'";
}

bool IcebergSqlMetastoreCommit::NamespaceExists(const vector<string> &namespace_items) {
	auto rows = IcebergSqlCatalog::Query(
	    *connection,
	    StringUtil::Format("SELECT 1 FROM %s WHERE catalog_name = $1 AND namespace = $2 "
	                       "UNION ALL SELECT 1 FROM %s WHERE catalog_name = $1 AND table_namespace = $2 LIMIT 1",
	                       catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE),
	                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
	    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(namespace_items))});
	return !rows.empty();
}

void IcebergSqlMetastoreCommit::CreateNamespace(const vector<string> &namespace_items) {
	// The JDBC catalog records namespaces without other properties with 'exists' = 'true'
	IcebergSqlCatalog::Query(*connection,
	                         StringUtil::Format("INSERT INTO %s (catalog_name, namespace, property_key, property_value) "
	                                            "VALUES ($1, $2, 'exists', 'true')",
	                                            catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE)),
	                         {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(namespace_items))});
}

void IcebergSqlMetastoreCommit::UpdateNamespaceProperties(const vector<string> &namespace_items,
                                                          const set<string> &removals,
                                                          const case_insensitive_map_t<string> &updates) {
	auto &catalog_name = catalog.GetCatalogName();
	auto namespace_name = IcebergSqlCatalog::NamespaceName(namespace_items);
	auto properties_table = catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE);
	auto delete_property = StringUtil::Format(
	    "DELETE FROM %s WHERE catalog_name = $1 AND namespace = $2 AND property_key = $3", properties_table);
	for (auto &key : removals) {
		IcebergSqlCatalog::Query(*connection, delete_property,
		                         {Value(catalog_name), Value(namespace_name), Value(key)});
	}
	for (auto &property : updates) {
		IcebergSqlCatalog::Query(*connection, delete_property,
		                         {Value(catalog_name), Value(namespace_name), Value(property.first)});
		IcebergSqlCatalog::Query(*connection,
		                         StringUtil::Format("INSERT INTO %s (catalog_name, namespace, property_key, "
		                                            "property_value) VALUES ($1, $2, $3, $4)",
		                                            properties_table),
		                         {Value(catalog_name), Value(namespace_name), Value(property.first),
		                          Value(property.second)});
	}
}

void IcebergSqlMetastoreCommit::DropNamespace(const vector<string> &namespace_items) {
	auto &catalog_name = catalog.GetCatalogName();
	auto namespace_name = IcebergSqlCatalog::NamespaceName(namespace_items);
	auto tables = IcebergSqlCatalog::Query(
	    *connection,
	    StringUtil::Format("SELECT 1 FROM %s WHERE catalog_name = $1 AND table_namespace = $2 LIMIT 1",
	                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE)),
	    {Value(catalog_name), Value(namespace_name)});
	if (!tables.empty()) {
		throw CatalogException("Namespace %s is not empty", namespace_name);
	}
	IcebergSqlCatalog::Query(*connection,
	                         StringUtil::Format("DELETE FROM %s WHERE catalog_name = $1 AND namespace = $2",
	                                            catalog.CatalogTable(IcebergSqlCatalog::NAMESPACE_PROPERTIES_TABLE)),
	                         {Value(catalog_name), Value(namespace_name)});
}

string IcebergSqlMetastoreCommit::LoadMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                       IcebergMetastoreEntryType type) {
	auto rows = IcebergSqlCatalog::Query(
	    *connection,
	    StringUtil::Format("SELECT metadata_location FROM %s "
	                       "WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3%s",
	                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE), TypeCondition(type)),
	    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(identifier.namespace_items)),
	     Value(identifier.name)});
	if (rows.empty() || rows[0][0].IsNull()) {
		return string();
	}
	return rows[0][0].ToString();
}

void IcebergSqlMetastoreCommit::CreateEntry(const IcebergMetastoreIdentifier &identifier,
                                            IcebergMetastoreEntryType type, const string &metadata_location,
                                            optional_ptr<const IcebergTableMetadata> metadata) {
	const bool has_type = catalog.HasIcebergTypeColumn();
	if (!has_type && type == IcebergMetastoreEntryType::VIEW) {
		throw NotImplementedException("Iceberg SQL catalogs with the V0 schema of the JDBC catalog can't store views");
	}
	try {
		IcebergSqlCatalog::Query(
		    *connection,
		    StringUtil::Format("INSERT INTO %s (catalog_name, table_namespace, table_name, metadata_location, "
		                       "previous_metadata_location%s) VALUES ($1, $2, $3, $4, NULL%s)",
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE), has_type ? ", iceberg_type" : "",
		                       has_type ? StringUtil::Format(", '%s'", TypeName(type)) : string()),
		    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(identifier.namespace_items)),
		     Value(identifier.name), Value(metadata_location)});
	} catch (ConstraintException &) {
		if (type == IcebergMetastoreEntryType::VIEW) {
			throw CatalogException("A table or view with name \"%s\" already exists", identifier.name);
		}
		throw CatalogException("Table with name \"%s\" already exists", identifier.name);
	}
}

void IcebergSqlMetastoreCommit::SwapMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                     IcebergMetastoreEntryType type, const string &base_location,
                                                     const string &new_location,
                                                     optional_ptr<const IcebergTableMetadata> metadata) {
	auto changed = ChangedRows(IcebergSqlCatalog::Query(
	    *connection,
	    StringUtil::Format("UPDATE %s SET metadata_location = $1, previous_metadata_location = $2 "
	                       "WHERE catalog_name = $3 AND table_namespace = $4 AND table_name = $5 "
	                       "AND metadata_location = $2%s",
	                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE), TypeCondition(type)),
	    {Value(new_location), Value(base_location), Value(catalog.GetCatalogName()),
	     Value(IcebergSqlCatalog::NamespaceName(identifier.namespace_items)), Value(identifier.name)}));
	if (changed != 1) {
		throw TransactionException("%s %s was changed by a concurrent transaction", EntryKind(type),
		                           identifier.ToString());
	}
}

void IcebergSqlMetastoreCommit::DropEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
                                          optional_ptr<const IcebergTableMetadata> metadata) {
	auto changed = ChangedRows(IcebergSqlCatalog::Query(
	    *connection,
	    StringUtil::Format(
	        "DELETE FROM %s WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3%s",
	        catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE), TypeCondition(type)),
	    {Value(catalog.GetCatalogName()), Value(IcebergSqlCatalog::NamespaceName(identifier.namespace_items)),
	     Value(identifier.name)}));
	if (changed != 1) {
		throw CatalogException("%s with name \"%s\" does not exist", EntryKind(type), identifier.name);
	}
}

void IcebergSqlMetastoreCommit::RenameEntry(const IcebergMetastoreIdentifier &identifier,
                                            IcebergMetastoreEntryType type, const string &new_name) {
	idx_t changed;
	try {
		changed = ChangedRows(IcebergSqlCatalog::Query(
		    *connection,
		    StringUtil::Format("UPDATE %s SET table_name = $1 "
		                       "WHERE catalog_name = $2 AND table_namespace = $3 AND table_name = $4%s",
		                       catalog.CatalogTable(IcebergSqlCatalog::TABLES_TABLE), TypeCondition(type)),
		    {Value(new_name), Value(catalog.GetCatalogName()),
		     Value(IcebergSqlCatalog::NamespaceName(identifier.namespace_items)), Value(identifier.name)}));
	} catch (ConstraintException &) {
		throw CatalogException("%s with name \"%s\" already exists", EntryKind(type), new_name);
	}
	if (changed != 1) {
		throw CatalogException("%s with name \"%s\" does not exist", EntryKind(type), identifier.name);
	}
}

void IcebergSqlMetastoreCommit::Finish() {
	connection->Commit();
}

void IcebergSqlMetastoreCommit::Abort() {
	if (connection && connection->HasActiveTransaction()) {
		(void)connection->Query("ROLLBACK");
	}
}

} // namespace duckdb
