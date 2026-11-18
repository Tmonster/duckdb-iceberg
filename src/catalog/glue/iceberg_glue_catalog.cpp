#include "catalog/glue/iceberg_glue_catalog.hpp"

#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/string_util.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/glue/iceberg_glue_commit.hpp"

namespace duckdb {

IcebergGlueCatalog::IcebergGlueCatalog(AttachedDatabase &db_p, AccessMode access_mode,
                                       IcebergAttachOptions &attach_options_p,
                                       const optional<Identifier> &default_schema,
                                       IcebergGlueClientOptions client_options)
    : IcebergMetastoreCatalog(IcebergCatalogType::GLUE, db_p, access_mode, attach_options_p, default_schema),
      client(std::move(client_options)) {
	// Glue has no nested namespaces
	namespace_separator = ".";
}

IcebergGlueCatalog::~IcebergGlueCatalog() = default;

string IcebergGlueCatalog::DatabaseName(const vector<string> &namespace_items) {
	if (namespace_items.size() != 1) {
		throw InvalidInputException("AWS Glue has no nested namespaces, '%s' is not a Glue database",
		                            StringUtil::Join(namespace_items, "."));
	}
	return namespace_items[0];
}

string IcebergGlueCatalog::Parameter(JSONValue entry, const string &key) {
	auto parameters = entry.GetMember("Parameters");
	if (!parameters.IsValid() || !parameters.IsObject()) {
		return string();
	}
	auto value = parameters.GetMember(key);
	if (!value.IsValid() || !value.IsString()) {
		return string();
	}
	return value.GetString();
}

bool IcebergGlueCatalog::IsIcebergTable(JSONValue table) {
	return StringUtil::CIEquals(Parameter(table, TABLE_TYPE_PARAMETER), ICEBERG_TABLE_TYPE);
}

unique_ptr<JSONDocument> IcebergGlueCatalog::GetDatabase(ClientContext &context, const string &database_name) {
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("Name", database_name);
	try {
		return client.Call(context, "GetDatabase", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "EntityNotFoundException") {
			return nullptr;
		}
		throw;
	}
}

unique_ptr<JSONDocument> IcebergGlueCatalog::GetTable(ClientContext &context,
                                                      const IcebergMetastoreIdentifier &identifier) {
	JSONWriter request;
	auto body = request.CreateObject();
	body.AddString("DatabaseName", DatabaseName(identifier.namespace_items));
	body.AddString("Name", identifier.name);
	try {
		return client.Call(context, "GetTable", request, body);
	} catch (IcebergGlueException &ex) {
		if (ex.error_type == "EntityNotFoundException") {
			return nullptr;
		}
		throw;
	}
}

void IcebergGlueCatalog::ListPages(ClientContext &context, const string &operation, const string &list_key,
                                   const std::function<void(JSONWriter &, JSONMutableValue &)> &build_request,
                                   const std::function<void(JSONValue)> &callback) {
	string next_token;
	do {
		JSONWriter request;
		auto body = request.CreateObject();
		build_request(request, body);
		if (!next_token.empty()) {
			body.AddString("NextToken", next_token);
		}
		auto response = client.Call(context, operation, request, body);
		auto root = response->GetRoot();
		auto entries = root.GetMember(list_key);
		if (entries.IsValid() && entries.IsArray()) {
			entries.IterateArray(callback);
		}
		auto token = root.GetMember("NextToken");
		next_token = token.IsValid() && token.IsString() ? token.GetString() : string();
	} while (!next_token.empty());
}

//===--------------------------------------------------------------------===//
// Reads
//===--------------------------------------------------------------------===//

bool IcebergGlueCatalog::SchemaExists(ClientContext &context, const string &schema_name) {
	return GetDatabase(context, schema_name) != nullptr;
}

IcebergListSchemasResult IcebergGlueCatalog::ListSchemas(ClientContext &context, const vector<string> &parent) {
	IcebergListSchemasResult result;
	if (!parent.empty()) {
		return result;
	}
	ListPages(
	    context, "GetDatabases", "DatabaseList", [](JSONWriter &, JSONMutableValue &) {},
	    [&](JSONValue database) {
		    IRCAPISchema schema;
		    schema.catalog_name = GetName().GetIdentifierName();
		    schema.items.push_back(database.GetMember("Name").GetString());
		    result.push_back(std::move(schema));
	    });
	return result;
}

APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>>
IcebergGlueCatalog::LoadSchemaProperties(ClientContext &context, const IcebergSchemaEntry &schema) {
	APIResult<unique_ptr<const rest_api_objects::GetNamespaceResponse>> result;
	auto database = GetDatabase(context, DatabaseName(schema.namespace_items));
	if (!database) {
		result.status_ = HTTPStatusCode::NotFound_404;
		rest_api_objects::IcebergErrorResponse error;
		error._error.type = "NoSuchNamespaceException";
		error._error.message =
		    StringUtil::Format("Namespace does not exist: %s", StringUtil::Join(schema.namespace_items, "."));
		error._error.code = 404;
		result.error_ = std::move(error);
		return result;
	}
	auto response = make_uniq<rest_api_objects::GetNamespaceResponse>();
	response->_namespace.value = schema.namespace_items;
	response->properties = case_insensitive_map_t<string>();
	auto root = database->GetRoot().GetMember("Database");
	auto parameters = root.GetMember("Parameters");
	if (parameters.IsValid() && parameters.IsObject()) {
		parameters.IterateObject([&](const string &key, JSONValue value) {
			if (value.IsString()) {
				response->properties->emplace(key, value.GetString());
			}
		});
	}
	// Like GlueCatalog of Iceberg Java, the database's LocationUri is its 'location' and Description its 'comment'
	auto location = root.GetMember("LocationUri");
	if (location.IsValid() && location.IsString() && !location.GetString().empty()) {
		(*response->properties)["location"] = location.GetString();
	} else if (!GetWarehouse().empty()) {
		// Report where the database's tables are created by default, like the SQL catalog does
		(*response->properties)["location"] = DefaultDatabaseLocation(DatabaseName(schema.namespace_items));
	}
	auto description = root.GetMember("Description");
	if (description.IsValid() && description.IsString() && !description.GetString().empty()) {
		(*response->properties)["comment"] = description.GetString();
	}
	result.status_ = HTTPStatusCode::OK_200;
	result.result_ = std::move(response);
	return result;
}

IcebergListTablesResult IcebergGlueCatalog::ListTables(ClientContext &context, const vector<string> &namespace_items,
                                                       bool allow_forbidden) {
	vector<rest_api_objects::TableIdentifier> result;
	auto database_name = DatabaseName(namespace_items);
	try {
		ListPages(
		    context, "GetTables", "TableList",
		    [&](JSONWriter &, JSONMutableValue &body) { body.AddString("DatabaseName", database_name); },
		    [&](JSONValue table) {
			    // Glue also holds Hive, Delta and Hudi tables, only Iceberg tables are listed
			    if (!IsIcebergTable(table)) {
				    return;
			    }
			    rest_api_objects::TableIdentifier identifier;
			    identifier._namespace.value = namespace_items;
			    identifier.name = table.GetMember("Name").GetString();
			    result.push_back(std::move(identifier));
		    });
	} catch (IcebergGlueException &ex) {
		// A database that doesn't exist has no tables
		if (ex.error_type != "EntityNotFoundException") {
			throw;
		}
	}
	return result;
}

bool IcebergGlueCatalog::SupportsViews(IcebergViewOperation operation) const {
	return false;
}

IcebergListViewsResult IcebergGlueCatalog::ListViews(ClientContext &context, const vector<string> &namespace_items) {
	return vector<rest_api_objects::TableIdentifier>();
}

string IcebergGlueCatalog::GetMetadataLocation(ClientContext &context, const IcebergMetastoreIdentifier &identifier,
                                               IcebergMetastoreEntryType type) {
	if (type != IcebergMetastoreEntryType::TABLE) {
		return string();
	}
	auto response = GetTable(context, identifier);
	if (!response) {
		return string();
	}
	auto table = response->GetRoot().GetMember("Table");
	if (!IsIcebergTable(table)) {
		return string();
	}
	return Parameter(table, METADATA_LOCATION_PARAMETER);
}

//===--------------------------------------------------------------------===//
// Statement time
//===--------------------------------------------------------------------===//

string IcebergGlueCatalog::DefaultTableLocation(ClientContext &context, const vector<string> &namespace_items,
                                                const string &table_name) {
	// Like GlueCatalog of Iceberg Java: '<database LocationUri>/<table>', else '<warehouse>/<database>.db/<table>'
	auto database_name = DatabaseName(namespace_items);
	auto database = GetDatabase(context, database_name);
	if (database) {
		auto location = database->GetRoot().GetMember("Database").GetMember("LocationUri");
		if (location.IsValid() && location.IsString() && !location.GetString().empty()) {
			auto result = location.GetString();
			StringUtil::RTrim(result, "/");
			return result + "/" + table_name;
		}
	}
	if (GetWarehouse().empty()) {
		throw InvalidInputException("Cannot create table %s.%s without a 'location': the Glue database has no "
		                            "LocationUri, and the catalog has no DEFAULT_LOCATION",
		                            database_name, table_name);
	}
	return DefaultDatabaseLocation(database_name) + "/" + table_name;
}

string IcebergGlueCatalog::DefaultDatabaseLocation(const string &database_name) const {
	auto warehouse_root = GetWarehouse();
	StringUtil::RTrim(warehouse_root, "/");
	return warehouse_root + "/" + database_name + ".db";
}

void IcebergGlueCatalog::VerifyTableUpdates(const IcebergTransactionAlterUpdate &alter_update) const {
	VerifySingleTableUpdate(alter_update);
}

unique_ptr<IcebergMetastoreCommit> IcebergGlueCatalog::BeginCommit(ClientContext &context) {
	return make_uniq<IcebergGlueMetastoreCommit>(*this, context);
}

} // namespace duckdb
