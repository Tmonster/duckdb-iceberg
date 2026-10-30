#pragma once

#include "catalog/iceberg_catalog.hpp"

namespace duckdb {

//! An Iceberg catalog that is only a directory tree, in the layout of the Hadoop catalog of Iceberg Java:
//! '<warehouse>/<namespace levels>/<table>/metadata/v<N>.metadata.json', with 'version-hint.text' holding the current
//! version N. A directory is a table if it has a 'metadata' directory, and a namespace otherwise.
//!
//! There is no catalog service: DuckDB writes the metadata files itself (see IcebergClientCommit). A commit creates
//! the next version's file exclusively, so a concurrent commit of the same version fails. Without a way to publish
//! several files atomically, a transaction can only change one table, and namespace changes are applied one after
//! another. The catalog is meant for a single writer.
class IcebergFilesystemCatalog : public IcebergCatalog {
public:
	static constexpr const IcebergCatalogType TYPE = IcebergCatalogType::FILESYSTEM;
	static constexpr const char *VERSION_HINT_FILE = "version-hint.text";

public:
	IcebergFilesystemCatalog(AttachedDatabase &db_p, AccessMode access_mode, IcebergAttachOptions &attach_options,
	                         const optional<Identifier> &default_schema);
	~IcebergFilesystemCatalog() override;

public:
	bool SchemaExists(ClientContext &context, const string &schema_name) override;
	IcebergListSchemasResult ListSchemas(ClientContext &context, const vector<string> &parent) override;
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
	LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) override;
	IcebergListTablesResult ListTables(ClientContext &context, const vector<string> &namespace_items,
	                                   bool allow_forbidden) override;
	IcebergLoadTableResult LoadTable(ClientContext &context, const vector<string> &namespace_items,
	                                 const string &table_name) override;
	bool SupportsViews(IcebergViewOperation operation) const override;
	IcebergListViewsResult ListViews(ClientContext &context, const vector<string> &namespace_items) override;
	IcebergLoadViewResult LoadView(ClientContext &context, const vector<string> &namespace_items,
	                               const string &view_name) override;
	IcebergTable &CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
	                          const IcebergCreateTableRequest &request) override;
	void VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const override;
	void Commit(IcebergTransaction &transaction) override;

public:
	//! '<warehouse>/<namespace levels>'
	string NamespaceDirectory(const vector<string> &namespace_items) const;
	//! '<warehouse>/<namespace levels>/<table>'
	string TableDirectory(const vector<string> &namespace_items, const string &table_name) const;
	//! '<table directory>/metadata/v<version>.metadata.json'
	static string MetadataFile(FileSystem &fs, const string &table_directory, idx_t version);
	//! Whether 'directory' is a table, i.e. has a 'metadata' directory
	static bool IsTableDirectory(FileSystem &fs, const string &directory);
	//! The current version of a table: the version in its hint, or a newer one a commit wrote without updating the
	//! hint. Unset if the table has no metadata file.
	static optional_idx CurrentVersion(FileSystem &fs, const string &table_directory);
	//! Whether 'location' is inside the warehouse
	bool InWarehouse(const string &location) const;

private:
	//! The subdirectories of 'directory'
	static vector<string> ListDirectories(FileSystem &fs, const string &directory);
	string warehouse_root;
};

} // namespace duckdb
