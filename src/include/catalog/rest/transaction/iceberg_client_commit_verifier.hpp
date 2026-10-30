#pragma once

#include "duckdb/main/client_context.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

class IcebergRestCatalog;

//! Debug aid behind the 'iceberg_verify_client_commit' setting: every table commit sent to a REST catalog is also
//! applied client-side (see IcebergClientCommit), written to a metadata file and read back, and the result is
//! compared to the table metadata the server produced. Costs an extra table load per committed table.
class IcebergClientCommitVerifier {
public:
	static constexpr const char *SETTING_NAME = "iceberg_verify_client_commit";
	static bool IsEnabled(ClientContext &context);

	IcebergClientCommitVerifier(ClientContext &context, IcebergRestCatalog &catalog);

public:
	//! Before the request is sent: load the table's current state and apply 'request' to it client-side
	void Prepare(const rest_api_objects::CommitTableRequest &request);
	//! After the server accepted every prepared request. 'commit_response' is the body returned for a single table
	//! commit; without it the resulting metadata is loaded from the catalog.
	void Verify(optional_ptr<const string> commit_response);

private:
	struct PreparedTable {
		vector<string> namespace_items;
		string table_name;
		//! The metadata (file) the commit was based on, empty if the commit creates the table
		unique_ptr<IcebergTableMetadata> base;
		string base_metadata_location;
		unique_ptr<IcebergTableMetadata> expected;
		//! Set if the client-side commit rejected the request
		string error;
	};

	void VerifyTable(PreparedTable &table, const IcebergTableMetadata &actual);

private:
	ClientContext &context;
	IcebergRestCatalog &catalog;
	vector<PreparedTable> tables;
};

} // namespace duckdb
