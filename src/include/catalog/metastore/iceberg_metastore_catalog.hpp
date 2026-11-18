#pragma once

#include "catalog/iceberg_catalog.hpp"

namespace duckdb {

class IcebergTableMetadata;

//! What a metastore entry points at
enum class IcebergMetastoreEntryType : uint8_t { TABLE, VIEW };

//! A table or view of a metastore: its namespace and name
struct IcebergMetastoreIdentifier {
	vector<string> namespace_items;
	string name;

	string ToString() const;
};

//! One attempt to publish a transaction's changes to a metastore. IcebergMetastoreCatalog calls these in a fixed order
//! (namespace creates, namespace properties, table changes, view changes, namespace drops) and then Finish().
//!
//! A metastore with transactions (a SQL database) buffers the operations and publishes them together in Finish(). A
//! metastore without them (Glue, a file system) publishes each operation as it is called, and reports that with
//! IsAtomic(): a failure part way through then leaves the earlier operations in place.
//!
//! Conflicts with a concurrent writer are reported as a TransactionException, which the catalog retries when that is
//! safe. A CatalogException reports an entry that exists already or doesn't exist.
class IcebergMetastoreCommit {
public:
	virtual ~IcebergMetastoreCommit() = default;

public:
	//! Whether Finish() publishes all operations of the attempt together
	virtual bool IsAtomic() const = 0;

	//===--------------------------------------------------------------------===//
	// Namespaces
	//===--------------------------------------------------------------------===//
	virtual bool NamespaceExists(const vector<string> &namespace_items) = 0;
	virtual void CreateNamespace(const vector<string> &namespace_items) = 0;
	virtual void UpdateNamespaceProperties(const vector<string> &namespace_items, const set<string> &removals,
	                                       const case_insensitive_map_t<string> &updates);
	//! Throws a CatalogException if the namespace still has tables or views
	virtual void DropNamespace(const vector<string> &namespace_items) = 0;

	//===--------------------------------------------------------------------===//
	// Tables and views
	//===--------------------------------------------------------------------===//
	//! The location of the entry's current metadata file, empty if there is no such entry
	virtual string LoadMetadataLocation(const IcebergMetastoreIdentifier &identifier,
	                                    IcebergMetastoreEntryType type) = 0;
	//! Add an entry pointing at its first metadata file. 'metadata' is set for tables. Throws a CatalogException if
	//! a table or view with the name exists already.
	virtual void CreateEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                         const string &metadata_location, optional_ptr<const IcebergTableMetadata> metadata) = 0;
	//! Point an entry at 'new_location' if it still points at 'base_location', throws a TransactionException
	//! otherwise. 'metadata' is set for tables.
	virtual void SwapMetadataLocation(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                                  const string &base_location, const string &new_location,
	                                  optional_ptr<const IcebergTableMetadata> metadata) = 0;
	//! Throws a CatalogException if the entry doesn't exist. 'metadata' is set for tables.
	virtual void DropEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                       optional_ptr<const IcebergTableMetadata> metadata) = 0;
	//! Throws a CatalogException if the entry doesn't exist or the new name is taken
	virtual void RenameEntry(const IcebergMetastoreIdentifier &identifier, IcebergMetastoreEntryType type,
	                         const string &new_name);

	//===--------------------------------------------------------------------===//
	// End of the attempt
	//===--------------------------------------------------------------------===//
	virtual void Finish() = 0;
	//! Undo what can be undone after a failed attempt. Must not throw.
	virtual void Abort() = 0;

public:
	//! Whether a table or view change of this attempt may be visible to others: the attempt can't be retried then,
	//! and the transaction's files may be referenced
	bool HasPublished() const {
		return published;
	}
	void MarkPublished() {
		published = true;
	}

private:
	bool published = false;
};

//! Base class of the Iceberg catalogs whose service only stores a pointer to each table's current metadata file, like
//! BaseMetastoreCatalog of Iceberg Java: a SQL database (the JDBC catalog), AWS Glue, a file system. DuckDB writes the
//! metadata files itself (see IcebergClientCommit) and the metastore only swaps the pointers. A subclass implements the
//! listings, GetMetadataLocation and an IcebergMetastoreCommit, and this class implements loading tables, creating
//! tables and the commit sequence on top of them.
class IcebergMetastoreCatalog : public IcebergCatalog {
public:
	static constexpr const IcebergCatalogType TYPE = IcebergCatalogType::INVALID;

public:
	IcebergMetastoreCatalog(IcebergCatalogType catalog_type, AttachedDatabase &db_p, AccessMode access_mode,
	                        IcebergAttachOptions &attach_options, const optional<Identifier> &default_schema);
	~IcebergMetastoreCatalog() override;

public:
	IcebergLoadTableResult LoadTable(ClientContext &context, const vector<string> &namespace_items,
	                                 const string &table_name) override;
	IcebergTable &CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
	                          const IcebergCreateTableRequest &request) override;
	void Commit(IcebergTransaction &transaction) override;

public:
	//===--------------------------------------------------------------------===//
	// Metastore hooks
	//===--------------------------------------------------------------------===//
	//! A short name of the metastore for messages, e.g. "SQL catalog"
	virtual string MetastoreName() const = 0;
	//! The location of the entry's current metadata file, empty if there is no such entry
	virtual string GetMetadataLocation(ClientContext &context, const IcebergMetastoreIdentifier &identifier,
	                                   IcebergMetastoreEntryType type) = 0;
	//! The location for a new table or view that doesn't request one
	virtual string DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
	                                    const string &table_name) = 0;
	//! Start an attempt to publish a transaction's changes
	virtual unique_ptr<IcebergMetastoreCommit> BeginCommit(ClientContext &context) = 0;
	//! Write the metadata file of a table commit and return its location. By default this writes
	//! '<table location>/metadata/<version>-<uuid>.metadata.json'. A metastore whose metadata files are the pointer (a
	//! file system) creates the next version here, and throws a TransactionException if it exists already.
	virtual string WriteTableMetadata(ClientContext &context, IcebergMetastoreCommit &commit,
	                                  const IcebergMetastoreIdentifier &identifier, IcebergTableMetadata &metadata,
	                                  optional_ptr<const IcebergTableMetadata> base, const string &base_location);
	//! The directory to remove after dropping 'table', empty to keep its files. By default a table's files are only
	//! removed with PURGE_REQUESTED, and only if they are inside the warehouse.
	virtual string PurgeLocation(const IcebergTable &table) const;

public:
	//! Whether 'location' is inside the warehouse
	bool InWarehouse(const string &location) const;
	//! The metastore identifier of a schema's table or view
	static IcebergMetastoreIdentifier MetastoreIdentifier(const IcebergSchemaEntry &schema, const string &name);

protected:
	//! Throws if the transaction changes more than one table, for metastores that can't publish several table changes
	//! atomically
	void VerifySingleTableUpdate(const IcebergTransactionAlterUpdate &alter_update) const;
};

} // namespace duckdb
