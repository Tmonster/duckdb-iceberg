#include "catalog/filesystem/iceberg_filesystem_catalog.hpp"

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/exception.hpp"
#ifdef _WIN32
#include "duckdb/common/windows.hpp"
#include "duckdb/common/windows_util.hpp"
#else
#include <cerrno>
#include <cstring>
#include <unistd.h>
#endif

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

bool IcebergFilesystemCatalog::HasMetadataDirectory(FileSystem &fs, const string &directory) {
	return fs.DirectoryExists(fs.JoinPath(directory, "metadata"));
}

bool IcebergFilesystemCatalog::IsTableDirectory(FileSystem &fs, const string &directory) {
	return HasMetadataDirectory(fs, directory) && CurrentVersion(fs, directory).IsValid();
}

optional_idx IcebergFilesystemCatalog::ParseVersionFile(const string &path) {
	static constexpr const char *SUFFIX = ".metadata.json";
	auto file_name = path.substr(path.find_last_of('/') + 1);
	if (!StringUtil::StartsWith(file_name, "v") || !StringUtil::EndsWith(file_name, SUFFIX)) {
		return optional_idx();
	}
	auto digits = file_name.substr(1, file_name.size() - 1 - strlen(SUFFIX));
	if (digits.empty() || digits.size() > 18 ||
	    !std::all_of(digits.begin(), digits.end(), [](char c) { return StringUtil::CharacterIsDigit(c); })) {
		return optional_idx();
	}
	return optional_idx(std::stoull(digits));
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
			// Temporary files of a commit in progress (or of one that stopped) aren't versions
			auto file_version = ParseVersionFile(name);
			if (file_version.IsValid() && (!version.IsValid() || file_version.GetIndex() > version.GetIndex())) {
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
	return fs.DirectoryExists(directory) && !HasMetadataDirectory(fs, directory);
}

IcebergListSchemasResult IcebergFilesystemCatalog::ListSchemas(ClientContext &context, const vector<string> &parent) {
	auto &fs = FileSystem::GetFileSystem(context);
	IcebergListSchemasResult result;
	auto directory = parent.empty() ? warehouse_root : NamespaceDirectory(parent);
	for (auto &name : ListDirectories(fs, directory)) {
		auto items = parent;
		items.push_back(name);
		if (HasMetadataDirectory(fs, NamespaceDirectory(items))) {
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

//! Atomically creates 'path' as a hard link to the complete file 'temp_path', returns false if 'path' exists
//! already. DuckDB's FileSystem::MoveFile is rename(), which replaces an existing target, so it can't arbitrate
//! between commits: link() (MoveFileW on Windows) fails instead. Both are atomic on a local file system.
bool PublishFile(const string &temp_path, const string &path) {
#ifdef _WIN32
	auto unicode_temp = WindowsUtil::UTF8ToUnicode(temp_path.c_str());
	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	if (MoveFileW(unicode_temp.c_str(), unicode_path.c_str())) {
		return true;
	}
	auto error = GetLastError();
	if (error == ERROR_ALREADY_EXISTS || error == ERROR_FILE_EXISTS) {
		return false;
	}
	throw IOException("Could not publish metadata file \"%s\" as \"%s\" (error %d)", temp_path, path, error);
#else
	if (link(temp_path.c_str(), path.c_str()) == 0) {
		return true;
	}
	if (errno == EEXIST) {
		return false;
	}
	throw IOException("Could not publish metadata file \"%s\" as \"%s\": %s", temp_path, path, strerror(errno));
#endif
}

//! Writes 'content' to a temporary file next to 'path', then publishes it as 'path' if that doesn't exist yet.
//! Readers and crashes never leave a partial 'path': it appears complete, or not at all. Returns false if 'path'
//! exists already.
bool WriteNewFile(FileSystem &fs, const string &path, const string &content) {
	if (fs.FileExists(path)) {
		return false;
	}
	auto slash = path.find_last_of('/');
	auto directory = path.substr(0, slash);
	// Not a 'v<N>.metadata.json' name, so CurrentVersion never takes it for a version
	auto temp_path = directory + "/." + path.substr(slash + 1) + "." + UUID::ToString(UUID::GenerateRandomUUID()) + ".tmp";
	{
		auto file = fs.OpenFile(temp_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		file->Write((void *)content.c_str(), content.size());
		// The content must be durable before it is published, or a crash could leave a committed partial version
		file->Sync();
		file->Close();
	}
	bool published;
	try {
		published = PublishFile(fs.ExpandPath(temp_path), fs.ExpandPath(path));
	} catch (...) {
		fs.TryRemoveFile(temp_path);
		throw;
	}
	// On success the final name is a second link to the file, on failure the temporary file is all that's left
	fs.TryRemoveFile(temp_path);
	return published;
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
	fs.CreateDirectoriesRecursive(metadata_directory);
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
