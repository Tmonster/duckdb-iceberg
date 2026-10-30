#include "catalog/rest/transaction/iceberg_client_commit_verifier.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"

#include "catalog/rest/iceberg_rest_catalog.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"

namespace duckdb {

bool IcebergClientCommitVerifier::IsEnabled(ClientContext &context) {
	Value value;
	return context.TryGetCurrentSetting(SETTING_NAME, value) && !value.IsNull() && value.GetValue<bool>();
}

IcebergClientCommitVerifier::IcebergClientCommitVerifier(ClientContext &context, IcebergRestCatalog &catalog)
    : context(context), catalog(catalog) {
}

static string TableName(const vector<string> &namespace_items, const string &table_name) {
	return StringUtil::Join(namespace_items, ".") + "." + table_name;
}

void IcebergClientCommitVerifier::Prepare(const rest_api_objects::CommitTableRequest &request) {
	if (!request.identifier) {
		throw InternalException("Cannot verify a table commit without a table identifier");
	}
	PreparedTable table;
	table.namespace_items = request.identifier->_namespace.value;
	table.table_name = request.identifier->name;

	bool creates_table = false;
	for (auto &requirement : request.requirements) {
		creates_table |= requirement.assert_create.has_value();
	}
	if (!creates_table) {
		auto load_result = catalog.LoadTable(context, table.namespace_items, table.table_name);
		if (load_result.error_) {
			throw InvalidConfigurationException("%s: could not load table %s before the commit: %s", SETTING_NAME,
			                        TableName(table.namespace_items, table.table_name),
			                        load_result.error_->_error.message);
		}
		auto &load_table_result = *load_result.result_;
		table.base =
		    make_uniq<IcebergTableMetadata>(IcebergTableMetadata::FromTableMetadata(load_table_result.metadata));
		if (load_table_result.metadata_location) {
			table.base_metadata_location = *load_table_result.metadata_location;
		}
	}
	try {
		table.expected = make_uniq<IcebergTableMetadata>(IcebergClientCommit::Apply(table.base.get(), request));
	} catch (std::exception &ex) {
		table.error = ErrorData(ex).RawMessage();
	}
	tables.push_back(std::move(table));
}

void IcebergClientCommitVerifier::VerifyTable(PreparedTable &table, const IcebergTableMetadata &actual) {
	auto name = TableName(table.namespace_items, table.table_name);
	if (!table.expected) {
		throw InvalidConfigurationException(
		    "%s: the catalog accepted a commit to %s that the client-side commit rejected: %s",
		                        SETTING_NAME, name, table.error);
	}
	auto differences = IcebergClientCommit::Compare(*table.expected, actual);

	// Write the client-side result next to the table's metadata and read it back, to verify the metadata file too
	auto &fs = FileSystem::GetFileSystem(context);
	auto written = table.expected->Copy();
	auto path = IcebergClientCommit::WriteMetadataFile(context, written, table.base.get(),
	                                                   table.base_metadata_location, written.GetMetadataPath(fs));
	try {
		auto reread = IcebergTableMetadata::FromTableMetadata(IcebergTableMetadata::Parse(path, fs, "none"));
		for (auto &difference : IcebergClientCommit::Compare(reread, actual)) {
			differences.push_back("in the written metadata file, " + difference);
		}
		if (reread.metadata_log.size() != written.metadata_log.size()) {
			differences.push_back(StringUtil::Format("in the written metadata file, metadata-log has %d entries "
			                                         "instead of %d",
			                                         reread.metadata_log.size(), written.metadata_log.size()));
		} else if (table.base && !table.base_metadata_location.empty()) {
			// The commit's base is the newest entry, recorded with the time it became current
			auto &newest = reread.metadata_log.back();
			if (newest.metadata_file != table.base_metadata_location ||
			    newest.timestamp_ms.value != table.base->last_updated_ms.value) {
				differences.push_back(StringUtil::Format(
				    "in the written metadata file, the newest metadata-log entry is ('%s', %d) instead of ('%s', %d)",
				    newest.metadata_file, newest.timestamp_ms.value, table.base_metadata_location,
				    table.base->last_updated_ms.value));
			}
		}
	} catch (...) {
		fs.TryRemoveFile(path);
		throw;
	}
	fs.TryRemoveFile(path);

	if (!differences.empty()) {
		throw InvalidConfigurationException("%s: the client-side commit of %s differs from the catalog's result:\n%s",
		                        SETTING_NAME, name, StringUtil::Join(differences, "\n"));
	}
}

void IcebergClientCommitVerifier::Verify(optional_ptr<const string> commit_response) {
	if (commit_response && tables.size() == 1 && !commit_response->empty()) {
		auto doc = ICUtils::APIResultToDoc(*commit_response);
		auto response = rest_api_objects::CommitTableResponse::FromJSON(doc->GetRoot());
		VerifyTable(tables[0], IcebergTableMetadata::FromTableMetadata(response.metadata));
		return;
	}
	for (auto &table : tables) {
		auto load_result = catalog.LoadTable(context, table.namespace_items, table.table_name);
		if (load_result.error_) {
			throw InvalidConfigurationException("%s: could not load table %s after the commit: %s", SETTING_NAME,
			                        TableName(table.namespace_items, table.table_name),
			                        load_result.error_->_error.message);
		}
		VerifyTable(table, IcebergTableMetadata::FromTableMetadata(load_result.result_->metadata));
	}
}

} // namespace duckdb
