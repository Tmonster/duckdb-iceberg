#pragma once

#include "catalog/metastore/iceberg_metastore_catalog.hpp"

namespace duckdb {

//! An Iceberg catalog that is only a directory tree, in the layout of the Hadoop catalog of Iceberg Java:
//! '<warehouse>/<namespace levels>/<table>/metadata/v<N>.metadata.json', with 'version-hint.text' holding the current
//! version N. A directory is a table if its 'metadata' directory holds a committed version, and a namespace if it has
//! no 'metadata' directory. A 'metadata' directory without a committed version (a creation that failed or was rolled
//! back) is neither.
//!
//! There is no catalog service: DuckDB writes the metadata files itself, and a table's newest metadata file is its
//! pointer (see IcebergMetastoreCatalog). A commit writes the next version's file completely under a temporary name,
//! then publishes it atomically under its final name, failing if that name exists already: readers never see a partial
//! version, and a concurrent commit of the same version fails. Only the local file system is supported, as object
//! stores have no such publication. Without a way to publish several files atomically, a transaction can only change one
//! table, and namespace changes are applied one after another. The catalog is meant for a single writer.
class IcebergFilesystemCatalog : public IcebergMetastoreCatalog {
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
	bool SupportsViews(IcebergViewOperation operation) const override;
	IcebergListViewsResult ListViews(ClientContext &context, const vector<string> &namespace_items) override;
	IcebergTable &CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
	                          const IcebergCreateTableRequest &request) override;
	void VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const override;

	string MetastoreName() const override {
		return "filesystem catalog";
	}
	string GetMetadataLocation(ClientContext &context, const IcebergMetastoreIdentifier &identifier,
	                           IcebergMetastoreEntryType type) override;
	string DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
	                            const string &table_name) override;
	unique_ptr<IcebergMetastoreCommit> BeginCommit(ClientContext &context) override;
	//! Publishes 'v<N>.metadata.json' of the next version atomically, if it doesn't exist yet: that is the commit
	string WriteTableMetadata(ClientContext &context, IcebergMetastoreCommit &commit,
	                          const IcebergMetastoreIdentifier &identifier, IcebergTableMetadata &metadata,
	                          optional_ptr<const IcebergTableMetadata> base, const string &base_location) override;
	//! A dropped table's directory is removed when it is dropped
	string PurgeLocation(const IcebergTable &table) const override {
		return string();
	}

public:
	//! '<warehouse>/<namespace levels>'
	string NamespaceDirectory(const vector<string> &namespace_items) const;
	//! '<warehouse>/<namespace levels>/<table>'
	string TableDirectory(const vector<string> &namespace_items, const string &table_name) const;
	//! '<table directory>/metadata/v<version>.metadata.json'
	static string MetadataFile(FileSystem &fs, const string &table_directory, idx_t version);
	//! Whether 'directory' is a table, i.e. its 'metadata' directory holds a committed version
	static bool IsTableDirectory(FileSystem &fs, const string &directory);
	//! Whether 'directory' has a 'metadata' directory, which makes it a table or a failed creation of one, never a
	//! namespace
	static bool HasMetadataDirectory(FileSystem &fs, const string &directory);
	//! The version of a committed metadata file name, 'v<N>.metadata.json'. Unset for other files, such as the
	//! temporary files a commit writes before it publishes one.
	static optional_idx ParseVersionFile(const string &path);
	//! The current version of a table: the version in its hint, or a newer one a commit wrote without updating the
	//! hint. Unset if the table has no metadata file.
	static optional_idx CurrentVersion(FileSystem &fs, const string &table_directory);

private:
	//! The subdirectories of 'directory'
	static vector<string> ListDirectories(FileSystem &fs, const string &directory);
	string warehouse_root;
};

} // namespace duckdb
