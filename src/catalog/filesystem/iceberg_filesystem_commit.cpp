#include "catalog/filesystem/iceberg_filesystem_commit.hpp"

#include "duckdb/common/string_util.hpp"

#include "catalog/filesystem/iceberg_filesystem_catalog.hpp"
#include "catalog/transaction/iceberg_client_commit.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergFilesystemMetastoreCommit::IcebergFilesystemMetastoreCommit(IcebergFilesystemCatalog &catalog,
                                                                   ClientContext &context)
    : catalog(catalog), context(context) {
}

namespace {

void OverwriteFile(FileSystem &fs, const string &path, const string &content) {
	auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	file->Write((void *)content.c_str(), content.size());
	file->Close();
}

} // namespace

bool IcebergFilesystemMetastoreCommit::NamespaceExists(const vector<string> &namespace_items) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = catalog.NamespaceDirectory(namespace_items);
	if (!fs.DirectoryExists(directory)) {
		return false;
	}
	if (IcebergFilesystemCatalog::IsTableDirectory(fs, directory)) {
		throw CatalogException("Cannot create schema \"%s\": the directory '%s' is a table",
		                       StringUtil::Join(namespace_items, "."), directory);
	}
	return true;
}

void IcebergFilesystemMetastoreCommit::CreateNamespace(const vector<string> &namespace_items) {
	FileSystem::GetFileSystem(context).CreateDirectoriesRecursive(catalog.NamespaceDirectory(namespace_items));
}

void IcebergFilesystemMetastoreCommit::UpdateNamespaceProperties(const vector<string> &namespace_items,
                                                                 const set<string> &removals,
                                                                 const case_insensitive_map_t<string> &updates) {
	throw NotImplementedException("Namespace properties are not supported by Iceberg filesystem catalogs");
}

void IcebergFilesystemMetastoreCommit::DropNamespace(const vector<string> &namespace_items) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = catalog.NamespaceDirectory(namespace_items);
	bool empty = true;
	fs.ListFiles(directory, [&](const string &, bool is_directory) {
		if (is_directory) {
			empty = false;
		}
	});
	if (!empty) {
		throw CatalogException("Namespace %s is not empty", StringUtil::Join(namespace_items, "."));
	}
	fs.RemoveDirectory(directory);
}

string IcebergFilesystemMetastoreCommit::LoadMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                              IcebergMetastoreEntryType type) {
	return catalog.GetMetadataLocation(context, identifier, type);
}

void IcebergFilesystemMetastoreCommit::WriteVersionHint(const IcebergMetastoreIdentifier &identifier,
                                                        const string &metadata_location) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto table_directory = catalog.TableDirectory(identifier.namespace_items, identifier.name);
	auto version = IcebergClientCommit::ParseMetadataVersion(metadata_location);
	OverwriteFile(fs, fs.JoinPath(fs.JoinPath(table_directory, "metadata"), IcebergFilesystemCatalog::VERSION_HINT_FILE),
	              std::to_string(version));
}

void IcebergFilesystemMetastoreCommit::CreateEntry(const IcebergMetastoreIdentifier &identifier,
                                                   IcebergMetastoreEntryType type, const string &metadata_location,
                                                   optional_ptr<const IcebergTableMetadata> metadata) {
	// The table exists since its first metadata file was created
	WriteVersionHint(identifier, metadata_location);
}

void IcebergFilesystemMetastoreCommit::SwapMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                            IcebergMetastoreEntryType type,
                                                            const string &base_location, const string &new_location,
                                                            optional_ptr<const IcebergTableMetadata> metadata) {
	// The new version was committed when its file was created, the hint only spares readers a search for it
	WriteVersionHint(identifier, new_location);
}

void IcebergFilesystemMetastoreCommit::DropEntry(const IcebergMetastoreIdentifier &identifier,
                                                 IcebergMetastoreEntryType type,
                                                 optional_ptr<const IcebergTableMetadata> metadata) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto table_directory = catalog.TableDirectory(identifier.namespace_items, identifier.name);
	if (!IcebergFilesystemCatalog::IsTableDirectory(fs, table_directory)) {
		throw CatalogException("Table with name \"%s\" does not exist", identifier.name);
	}
	// The table is its directory, so dropping it removes its files. Only files inside the warehouse are removed.
	if (metadata) {
		auto &location = metadata->GetLocation();
		if (!location.empty() && location != table_directory && !catalog.InWarehouse(location)) {
			throw NotImplementedException("Cannot drop table \"%s\": its location '%s' is outside the warehouse",
			                              identifier.name, location);
		}
	}
	fs.RemoveDirectory(table_directory);
}

void IcebergFilesystemMetastoreCommit::RenameEntry(const IcebergMetastoreIdentifier &identifier,
                                                   IcebergMetastoreEntryType type, const string &new_name) {
	throw NotImplementedException("Renaming tables is not supported by Iceberg filesystem catalogs: a table's "
	                              "directory and the paths in its metadata would all have to change");
}

} // namespace duckdb
