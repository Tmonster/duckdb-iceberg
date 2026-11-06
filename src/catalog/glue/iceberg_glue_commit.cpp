#include "catalog/glue/iceberg_glue_commit.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/glue/iceberg_glue_catalog.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"

namespace duckdb {

IcebergGlueMetastoreCommit::IcebergGlueMetastoreCommit(IcebergGlueCatalog &catalog, ClientContext &context)
    : catalog(catalog), context(context) {
}

string IcebergGlueMetastoreCommit::HiveTypeName(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::INTEGER:
		return "int";
	case LogicalTypeId::BIGINT:
		return "bigint";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp";
	case LogicalTypeId::BLOB:
		return "binary";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("decimal(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::LIST:
		return "array<" + HiveTypeName(ListType::GetChildType(type)) + ">";
	case LogicalTypeId::MAP:
		return "map<" + HiveTypeName(MapType::KeyType(type)) + "," + HiveTypeName(MapType::ValueType(type)) + ">";
	case LogicalTypeId::STRUCT: {
		vector<string> fields;
		for (auto &child : StructType::GetChildTypes(type)) {
			fields.push_back(child.first.GetIdentifierName() + ":" + HiveTypeName(child.second));
		}
		return "struct<" + StringUtil::Join(fields, ",") + ">";
	}
	default:
		// string, uuid, time
		return "string";
	}
}

namespace {

//! The members of a Glue Table that are part of a TableInput
const char *const TABLE_INPUT_MEMBERS[] = {"Description",      "Owner",      "LastAccessTime",   "LastAnalyzedTime",
                                           "Retention",        "PartitionKeys", "ViewOriginalText", "ViewExpandedText",
                                           "TableType",        "TargetTable"};

//! A TableInput for an Iceberg table: the members of 'existing' (if any), with the Iceberg parameters, and the location
//! and columns of 'metadata'
JSONMutableValue IcebergTableInput(JSONWriter &writer, optional_ptr<JSONValue> existing, const string &name,
                                   const string &metadata_location, const string &previous_metadata_location,
                                   optional_ptr<const IcebergTableMetadata> metadata) {
	auto input = writer.CreateObject();
	input.AddString("Name", name);
	bool has_table_type = false;
	if (existing) {
		for (auto member : TABLE_INPUT_MEMBERS) {
			auto value = existing->GetMember(member);
			if (value.IsValid() && !value.IsNull()) {
				input.Add(member, writer.CreateCopy(value));
				has_table_type = has_table_type || string(member) == "TableType";
			}
		}
	}
	if (!has_table_type) {
		input.AddString("TableType", "EXTERNAL_TABLE");
	}

	auto parameters = writer.CreateObject();
	if (existing) {
		auto existing_parameters = existing->GetMember("Parameters");
		if (existing_parameters.IsValid() && existing_parameters.IsObject()) {
			existing_parameters.IterateObject([&](const string &key, JSONValue value) {
				if (key == IcebergGlueCatalog::METADATA_LOCATION_PARAMETER ||
				    key == IcebergGlueCatalog::PREVIOUS_METADATA_LOCATION_PARAMETER ||
				    key == IcebergGlueCatalog::TABLE_TYPE_PARAMETER) {
					return;
				}
				parameters.Add(key, writer.CreateCopy(value));
			});
		}
	}
	parameters.AddString(IcebergGlueCatalog::TABLE_TYPE_PARAMETER, IcebergGlueCatalog::ICEBERG_TABLE_TYPE);
	parameters.AddString(IcebergGlueCatalog::METADATA_LOCATION_PARAMETER, metadata_location);
	if (!previous_metadata_location.empty()) {
		parameters.AddString(IcebergGlueCatalog::PREVIOUS_METADATA_LOCATION_PARAMETER, previous_metadata_location);
	}
	input.Add("Parameters", parameters);

	auto storage_descriptor = writer.CreateObject();
	if (metadata) {
		storage_descriptor.AddString("Location", metadata->GetLocation());
		vector<string> names;
		vector<LogicalType> types;
		metadata->GetLatestSchema().GetColumnNamesAndTypes(names, types);
		auto columns = writer.CreateArray();
		for (idx_t i = 0; i < names.size(); i++) {
			auto column = writer.CreateObject();
			column.AddString("Name", names[i]);
			column.AddString("Type", IcebergGlueMetastoreCommit::HiveTypeName(types[i]));
			columns.Append(column);
		}
		storage_descriptor.Add("Columns", columns);
	} else if (existing) {
		auto existing_descriptor = existing->GetMember("StorageDescriptor");
		if (existing_descriptor.IsValid() && existing_descriptor.IsObject()) {
			storage_descriptor = writer.CreateCopy(existing_descriptor);
		}
	}
	input.Add("StorageDescriptor", storage_descriptor);
	return input;
}

void ThrowAlreadyExists(const string &name) {
	throw CatalogException("Table with name \"%s\" already exists", name);
}

} // namespace

//===--------------------------------------------------------------------===//
// Namespaces
//===--------------------------------------------------------------------===//

bool IcebergGlueMetastoreCommit::NamespaceExists(const vector<string> &namespace_items) {
	return catalog.GetDatabase(context, IcebergGlueCatalog::DatabaseName(namespace_items)) != nullptr;
}

void IcebergGlueMetastoreCommit::CreateNamespace(const vector<string> &namespace_items) {
	JSONWriter request;
	auto body = request.CreateObject();
	auto database_input = request.CreateObject();
	database_input.AddString("Name", IcebergGlueCatalog::DatabaseName(namespace_items));
	body.Add("DatabaseInput", database_input);
	try {
		catalog.GetClient().Call(context, "CreateDatabase", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "AlreadyExistsException") {
			throw CatalogException("Schema with name \"%s\" already exists",
			                       IcebergGlueCatalog::DatabaseName(namespace_items));
		}
		throw;
	}
}

void IcebergGlueMetastoreCommit::UpdateNamespaceProperties(const vector<string> &namespace_items,
                                                           const set<string> &removals,
                                                           const case_insensitive_map_t<string> &updates) {
	auto database_name = IcebergGlueCatalog::DatabaseName(namespace_items);
	auto response = catalog.GetDatabase(context, database_name);
	if (!response) {
		throw CatalogException("Schema with name \"%s\" does not exist", database_name);
	}
	auto database = response->GetRoot().GetMember("Database");
	auto string_member = [&](const char *key) {
		auto value = database.GetMember(key);
		return value.IsValid() && value.IsString() ? value.GetString() : string();
	};
	auto location = string_member("LocationUri");
	auto description = string_member("Description");
	case_insensitive_map_t<string> parameters;
	auto existing_parameters = database.GetMember("Parameters");
	if (existing_parameters.IsValid() && existing_parameters.IsObject()) {
		existing_parameters.IterateObject([&](const string &key, JSONValue value) {
			if (value.IsString()) {
				parameters[key] = value.GetString();
			}
		});
	}
	for (auto &key : removals) {
		if (StringUtil::CIEquals(key, "location")) {
			location.clear();
		} else if (StringUtil::CIEquals(key, "comment")) {
			description.clear();
		} else {
			parameters.erase(key);
		}
	}
	for (auto &property : updates) {
		if (StringUtil::CIEquals(property.first, "location")) {
			location = property.second;
		} else if (StringUtil::CIEquals(property.first, "comment")) {
			description = property.second;
		} else {
			parameters[property.first] = property.second;
		}
	}

	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("Name", database_name);
	auto database_input = request.CreateObject();
	database_input.AddString("Name", database_name);
	if (!location.empty()) {
		database_input.AddString("LocationUri", location);
	}
	if (!description.empty()) {
		database_input.AddString("Description", description);
	}
	auto parameters_value = request.CreateObject();
	for (auto &parameter : parameters) {
		parameters_value.AddString(parameter.first, parameter.second);
	}
	database_input.Add("Parameters", parameters_value);
	body.Add("DatabaseInput", database_input);
	catalog.GetClient().Call(context, "UpdateDatabase", request, body);
}

void IcebergGlueMetastoreCommit::DropNamespace(const vector<string> &namespace_items) {
	auto database_name = IcebergGlueCatalog::DatabaseName(namespace_items);
	// DeleteDatabase drops the database's tables too, of any table format
	bool empty = true;
	catalog.ListPages(
	    context, "GetTables", "TableList",
	    [&](JSONWriter &, JSONMutableValue &body) {
		    body.AddString("DatabaseName", database_name);
	    },
	    [&](JSONValue) { empty = false; });
	if (!empty) {
		throw CatalogException("Namespace %s is not empty", database_name);
	}
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("Name", database_name);
	try {
		catalog.GetClient().Call(context, "DeleteDatabase", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "EntityNotFoundException") {
			throw CatalogException("Schema with name \"%s\" does not exist", database_name);
		}
		throw;
	}
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//

string IcebergGlueMetastoreCommit::LoadMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                        IcebergMetastoreEntryType type) {
	return catalog.GetMetadataLocation(context, identifier, type);
}

void IcebergGlueMetastoreCommit::CreateEntry(const IcebergMetastoreIdentifier &identifier,
                                             IcebergMetastoreEntryType type, const string &metadata_location,
                                             optional_ptr<const IcebergTableMetadata> metadata) {
	if (type != IcebergMetastoreEntryType::TABLE) {
		throw NotImplementedException("Views are not supported by Iceberg Glue catalogs");
	}
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("DatabaseName", IcebergGlueCatalog::DatabaseName(identifier.namespace_items));
	body.Add("TableInput", IcebergTableInput(request, nullptr, identifier.name, metadata_location, "", metadata));
	try {
		catalog.GetClient().Call(context, "CreateTable", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "AlreadyExistsException") {
			ThrowAlreadyExists(identifier.name);
		}
		throw;
	}
}

void IcebergGlueMetastoreCommit::SwapMetadataLocation(const IcebergMetastoreIdentifier &identifier,
                                                      IcebergMetastoreEntryType type, const string &base_location,
                                                      const string &new_location,
                                                      optional_ptr<const IcebergTableMetadata> metadata) {
	auto response = catalog.GetTable(context, identifier);
	if (!response) {
		throw TransactionException("Table %s was dropped by a concurrent transaction", identifier.ToString());
	}
	auto table = response->GetRoot().GetMember("Table");
	if (IcebergGlueCatalog::Parameter(table, IcebergGlueCatalog::METADATA_LOCATION_PARAMETER) != base_location) {
		throw TransactionException("Table %s was changed by a concurrent transaction", identifier.ToString());
	}
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("DatabaseName", IcebergGlueCatalog::DatabaseName(identifier.namespace_items));
	body.Add("TableInput",
	         IcebergTableInput(request, &table, identifier.name, new_location, base_location, metadata));
	// Glue rejects the update if the table changed since this version was read
	auto version_id = table.GetMember("VersionId");
	if (version_id.IsValid() && version_id.IsString()) {
		body.AddString("VersionId", version_id.GetString());
	}
	try {
		catalog.GetClient().Call(context, "UpdateTable", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "ConcurrentModificationException") {
			throw TransactionException("Table %s was changed by a concurrent transaction", identifier.ToString());
		}
		if (ex.error_type == "EntityNotFoundException") {
			throw TransactionException("Table %s was dropped by a concurrent transaction", identifier.ToString());
		}
		throw;
	}
}

void IcebergGlueMetastoreCommit::DropEntry(const IcebergMetastoreIdentifier &identifier,
                                           IcebergMetastoreEntryType type,
                                           optional_ptr<const IcebergTableMetadata> metadata) {
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("DatabaseName", IcebergGlueCatalog::DatabaseName(identifier.namespace_items));
	body.AddString("Name", identifier.name);
	try {
		catalog.GetClient().Call(context, "DeleteTable", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "EntityNotFoundException") {
			throw CatalogException("Table with name \"%s\" does not exist", identifier.name);
		}
		throw;
	}
}

void IcebergGlueMetastoreCommit::RenameEntry(const IcebergMetastoreIdentifier &identifier,
                                             IcebergMetastoreEntryType type, const string &new_name) {
	// Glue can't rename a table: like GlueCatalog of Iceberg Java, create the new table and drop the old one
	auto response = catalog.GetTable(context, identifier);
	if (!response) {
		throw CatalogException("Table with name \"%s\" does not exist", identifier.name);
	}
	auto table = response->GetRoot().GetMember("Table");
	auto metadata_location = IcebergGlueCatalog::Parameter(table, IcebergGlueCatalog::METADATA_LOCATION_PARAMETER);
	auto previous_location =
	    IcebergGlueCatalog::Parameter(table, IcebergGlueCatalog::PREVIOUS_METADATA_LOCATION_PARAMETER);
	auto database_name = IcebergGlueCatalog::DatabaseName(identifier.namespace_items);
	{
		JSONWriter request;
		auto body = request.CreateObject();
		body.AddString("DatabaseName", database_name);
		body.Add("TableInput",
		         IcebergTableInput(request, &table, new_name, metadata_location, previous_location, nullptr));
		try {
			catalog.GetClient().Call(context, "CreateTable", request, body);
		} catch (IcebergGlueException &ex) {
			if (ex.error_type == "AlreadyExistsException") {
				ThrowAlreadyExists(new_name);
			}
			throw;
		}
	}
	MarkPublished();
	DropEntry(identifier, type, nullptr);
}

} // namespace duckdb
