#pragma once

#include "duckdb/main/client_context.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

//! Commits a table change without a catalog server doing it for us: the commit's requirements are checked against
//! the table's current metadata, its updates are applied, and the resulting metadata is written to a new metadata
//! file. This is what an Iceberg REST catalog server does on 'POST .../tables/{table}'. Catalogs that only store a
//! pointer to each table's current metadata file (e.g. a SQL catalog) use this and then swap the pointer.
class IcebergClientCommit {
public:
	//! Check the requirements of 'request' against 'base' and apply its updates to a copy of it.
	//! 'base' is the table's current metadata, or nullptr if the table doesn't exist yet (the request then has to
	//! carry an 'assert-create' requirement). Throws a TransactionException if a requirement is not met.
	static IcebergTableMetadata Apply(optional_ptr<const IcebergTableMetadata> base,
	                                  const rest_api_objects::CommitTableRequest &request);
	static void CheckRequirements(optional_ptr<const IcebergTableMetadata> base,
	                              const vector<rest_api_objects::TableRequirement> &requirements);

	//! Write 'metadata' as the next version after 'previous' (nullptr for a new table), whose metadata file is
	//! 'previous_metadata_location', into 'metadata_directory'. The previous file is recorded in the metadata-log
	//! with its own last-updated-ms. Returns the new file's path.
	static string WriteMetadataFile(ClientContext &context, IcebergTableMetadata &metadata,
	                                optional_ptr<const IcebergTableMetadata> previous,
	                                const string &previous_metadata_location, const string &metadata_directory);
	//! The version of a metadata file named '<version>-<uuid>.metadata.json' or 'v<version>.metadata.json',
	//! or 0 if the name has neither format
	static idx_t ParseMetadataVersion(const string &metadata_location);

	//! Differences between the table state of 'expected' and 'actual', ignoring fields that legitimately differ
	//! between implementations (timestamps, metadata-log, properties added by a catalog).
	static vector<string> Compare(const IcebergTableMetadata &expected, const IcebergTableMetadata &actual);
};

} // namespace duckdb
