#include "catalog/filesystem/iceberg_filesystem_catalog.hpp"

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/catalog_entry/table/iceberg_table.hpp"
#include "catalog/filesystem/iceberg_filesystem_commit.hpp"
#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"
#include "catalog/transaction/iceberg_transaction_update.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergFilesystemCatalog::IcebergFilesystemCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                                                   IcebergAttachOptions &attach_options_p,
                                                   const optional<Identifier> &default_schema)
    : IcebergMetastoreCatalog(IcebergCatalogType::FILESYSTEM, db_p, access_mode, attach_options_p, default_schema),
      warehouse_root(attach_options_p.warehouse) {
	StringUtil::RTrim(warehouse_root, "/");
	// Nested namespaces are joined with '.' in DuckDB schema names, and are nested directories here
	namespace_separator = ".";
}

IcebergFilesystemCatalog::~IcebergFilesystemCatalog() = default;

//===--------------------------------------------------------------------===//
// Layout
//===--------------------------------------------------------------------===//

string IcebergFilesystemCatalog::NamespaceDirectory(const vector<string> &namespace_items) const {
	return warehouse_root + "/" + StringUtil::Join(namespace_items, "/");
}

string IcebergFilesystemCatalog::TableDirectory(const vector<string> &namespace_items, const string &table_name) const {
	return NamespaceDirectory(namespace_items) + "/" + table_name;
}

string IcebergFilesystemCatalog::MetadataFile(FileSystem &fs, const string &table_directory, idx_t version) {
	return fs.JoinPath(fs.JoinPath(table_directory, "metadata"), StringUtil::Format("v%d.metadata.json", version));
}

bool IcebergFilesystemCatalog::IsTableDirectory(FileSystem &fs, const string &directory) {
	return fs.DirectoryExists(fs.JoinPath(directory, "metadata"));
}

vector<string> IcebergFilesystemCatalog::ListDirectories(FileSystem &fs, const string &directory) {
	vector<string> result;
	if (!fs.DirectoryExists(directory)) {
		return result;
	}
	fs.ListFiles(directory, [&](const string &name, bool is_directory) {
		if (!is_directory) {
			return;
		}
		auto entry_name = name;
		StringUtil::RTrim(entry_name, "/");
		entry_name = entry_name.substr(entry_name.find_last_of('/') + 1);
		if (!entry_name.empty()) {
			result.push_back(entry_name);
		}
	});
	std::sort(result.begin(), result.end());
	return result;
}

optional_idx IcebergFilesystemCatalog::CurrentVersion(FileSystem &fs, const string &table_directory) {
	auto metadata_directory = fs.JoinPath(table_directory, "metadata");
	optional_idx version;
	auto hint_path = fs.JoinPath(metadata_directory, VERSION_HINT_FILE);
	if (fs.FileExists(hint_path)) {
		auto hint = IcebergUtils::FileToString(hint_path, fs);
		StringUtil::Trim(hint);
		if (!hint.empty() && std::all_of(hint.begin(), hint.end(), [](char c) { return StringUtil::CharacterIsDigit(c); })) {
			version = std::stoull(hint);
		}
	}
	if (!version.IsValid()) {
		// Without a (readable) hint, the newest metadata file is current
		fs.ListFiles(metadata_directory, [&](const string &name, bool is_directory) {
			if (is_directory) {
				return;
			}
			auto file_version = IcebergClientCommit::ParseMetadataVersion(name);
			auto file_name = name.substr(name.find_last_of('/') + 1);
			if (StringUtil::StartsWith(file_name, "v") && StringUtil::EndsWith(file_name, ".metadata.json") &&
			    (!version.IsValid() || file_version > version.GetIndex())) {
				version = file_version;
			}
		});
	}
	if (!version.IsValid()) {
		return version;
	}
	// A commit writes the metadata file before the hint, a writer may have stopped in between
	while (fs.FileExists(MetadataFile(fs, table_directory, version.GetIndex() + 1))) {
		version = version.GetIndex() + 1;
	}
	if (!fs.FileExists(MetadataFile(fs, table_directory, version.GetIndex()))) {
		return optional_idx();
	}
	return version;
}

//===--------------------------------------------------------------------===//
// Reads
//===--------------------------------------------------------------------===//

bool IcebergFilesystemCatalog::SchemaExists(ClientContext &context, const string &schema_name) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = NamespaceDirectory(StringUtil::Split(schema_name, '.'));
	return fs.DirectoryExists(directory) && !IsTableDirectory(fs, directory);
}

IcebergListSchemasResult IcebergFilesystemCatalog::ListSchemas(ClientContext &context, const vector<string> &parent) {
	auto &fs = FileSystem::GetFileSystem(context);
	IcebergListSchemasResult result;
	auto directory = parent.empty() ? warehouse_root : NamespaceDirectory(parent);
	for (auto &name : ListDirectories(fs, directory)) {
		auto items = parent;
		items.push_back(name);
		if (IsTableDirectory(fs, NamespaceDirectory(items))) {
			continue;
		}
		if (attach_options.support_nested_namespaces) {
			auto nested = ListSchemas(context, items);
			result.insert(result.end(), std::make_move_iterator(nested.begin()), std::make_move_iterator(nested.end()));
		}
		IRCAPISchema schema;
		schema.catalog_name = GetName().GetIdentifierName();
		schema.items = std::move(items);
		result.push_back(std::move(schema));
	}
	return result;
}

APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
IcebergFilesystemCatalog::LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) {
	// A namespace is only a directory: its location is its only property
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>> result;
	result.status_ = HTTPStatusCode::OK_200;
	auto response = make_uniq<rest_api_objects::GetNamespaceResponse>();
	response->_namespace.value = schema.namespace_items;
	response->properties = case_insensitive_map_t<string>();
	response->properties->emplace("location", NamespaceDirectory(schema.namespace_items));
	result.result_ = std::move(response);
	return result;
}

IcebergListTablesResult IcebergFilesystemCatalog::ListTables(ClientContext &context,
                                                             const vector<string> &namespace_items,
                                                             bool allow_forbidden) {
	auto &fs = FileSystem::GetFileSystem(context);
	vector<rest_api_objects::TableIdentifier> result;
	for (auto &name : ListDirectories(fs, NamespaceDirectory(namespace_items))) {
		if (!IsTableDirectory(fs, TableDirectory(namespace_items, name))) {
			continue;
		}
		rest_api_objects::TableIdentifier identifier;
		identifier._namespace.value = namespace_items;
		identifier.name = name;
		result.push_back(std::move(identifier));
	}
	return result;
}

bool IcebergFilesystemCatalog::SupportsViews(IcebergViewOperation operation) const {
	return false;
}

IcebergListViewsResult IcebergFilesystemCatalog::ListViews(ClientContext &context,
                                                           const vector<string> &namespace_items) {
	return vector<rest_api_objects::TableIdentifier>();
}

//===--------------------------------------------------------------------===//
// Statement time
//===--------------------------------------------------------------------===//

IcebergTable &IcebergFilesystemCatalog::CreateTable(ClientContext &context, IcebergSchemaEntry &schema,
                                                    const IcebergCreateTableRequest &request) {
	// The directory of a table is how it is found, so tables can't be stored elsewhere
	auto location = TableDirectory(schema.namespace_items, request.GetName());
	auto requested_location = request.GetLocation();
	StringUtil::RTrim(requested_location, "/");
	if (!requested_location.empty() && requested_location != location) {
		throw InvalidInputException("Tables of an Iceberg filesystem catalog are stored at '<warehouse>/<namespace>/"
		                            "<table>' ('%s'), a different 'location' is not supported",
		                            location);
	}
	return IcebergMetastoreCatalog::CreateTable(context, schema, request);
}

string IcebergFilesystemCatalog::DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
                                                      const string &table_name) {
	return TableDirectory(namespace_items, table_name);
}

void IcebergFilesystemCatalog::VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const {
	VerifySingleTableUpdate(alter_update);
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//

string IcebergFilesystemCatalog::GetMetadataLocation(ClientContext &context,
                                                     const IcebergMetastoreIdentifier &identifier,
                                                     IcebergMetastoreEntryType type) {
	if (type != IcebergMetastoreEntryType::TABLE) {
		return string();
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto table_directory = TableDirectory(identifier.namespace_items, identifier.name);
	if (!IsTableDirectory(fs, table_directory)) {
		return string();
	}
	auto version = CurrentVersion(fs, table_directory);
	if (!version.IsValid()) {
		return string();
	}
	return MetadataFile(fs, table_directory, version.GetIndex());
}

unique_ptr<IcebergMetastoreCommit> IcebergFilesystemCatalog::BeginCommit(ClientContext &context) {
	return make_uniq<IcebergFilesystemMetastoreCommit>(*this, context);
}

namespace {

//! Create 'path' with 'content', returns false if it exists already
bool WriteNewFile(FileSystem &fs, const string &path, const string &content) {
	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	             FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE | FileFlags::FILE_FLAGS_NULL_IF_EXISTS;
	if (fs.FileExists(path)) {
		return false;
	}
	unique_ptr<FileHandle> file;
	try {
		file = fs.OpenFile(path, flags);
	} catch (std::exception &) {
		// Not every file system returns NULL for an existing file
		if (fs.FileExists(path)) {
			return false;
		}
		throw;
	}
	if (!file) {
		return false;
	}
	file->Write((void *)content.c_str(), content.size());
	file->Close();
	return true;
}

} // namespace

string IcebergFilesystemCatalog::WriteTableMetadata(ClientContext &context, IcebergMetastoreCommit &commit,
                                                    const IcebergMetastoreIdentifier &identifier,
                                                    IcebergTableMetadata &metadata,
                                                    optional_ptr<const IcebergTableMetadata> base,
                                                    const string &base_location) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto table_directory = TableDirectory(identifier.namespace_items, identifier.name);
	auto metadata_directory = fs.JoinPath(table_directory, "metadata");
	if (!fs.IsRemoteFile(metadata_directory)) {
		fs.CreateDirectoriesRecursive(metadata_directory);
	}
	idx_t next_version = 1;
	if (base) {
		next_version = IcebergClientCommit::ParseMetadataVersion(base_location) + 1;
	} else if (CurrentVersion(fs, table_directory).IsValid()) {
		throw CatalogException("Table with name \"%s\" already exists", identifier.name);
	}
	IcebergClientCommit::RecordPreviousMetadata(metadata, base, base_location);
	auto metadata_location = MetadataFile(fs, table_directory, next_version);
	if (!WriteNewFile(fs, metadata_location, metadata.ToJSON())) {
		throw TransactionException("Table %s was changed by a concurrent commit (version %d exists already)",
		                           identifier.name, next_version);
	}
	// The new version is committed: the version hint only spares readers a search for it
	commit.MarkPublished();
	return metadata_location;
}

} // namespace duckdb
