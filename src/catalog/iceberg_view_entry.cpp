#include "catalog/iceberg_view_entry.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/query_result.hpp"

#include "common/iceberg_constants.hpp"
#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

UnsupportedIcebergViewEntry::UnsupportedIcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                         CreateViewInfo &info, string reason)
    : ViewCatalogEntry(catalog, schema, info), reason(std::move(reason)) {
}

const SelectStatement &UnsupportedIcebergViewEntry::GetQuery() {
	throw BinderException("Cannot query Iceberg view '%s': %s", name, reason);
}

void UnsupportedIcebergViewEntry::BindView(ClientContext &context, BindViewAction action) {
	GetQuery();
}

unique_ptr<CreateInfo> UnsupportedIcebergViewEntry::GetInfo() const {
	auto info = make_uniq<CreateViewInfo>(schema, name);
	info->sql = sql;
	info->aliases = aliases;
	return std::move(info);
}

string UnsupportedIcebergViewEntry::ToSQL() const {
	return sql;
}

unique_ptr<CatalogEntry> UnsupportedIcebergViewEntry::Copy(ClientContext &context) const {
	auto info = GetInfo();
	return make_uniq<UnsupportedIcebergViewEntry>(catalog, schema, info->Cast<CreateViewInfo>(), reason);
}


string CreateIcebergViewRequestJSON(const IcebergSchemaEntry &schema_entry, const CreateViewInfo &view_info_p) {
	auto view_info = &view_info_p;
	auto &view_name = view_info->GetViewName().GetIdentifierName();
	// query may have been moved by ViewCatalogEntry::Initialize(), so use sql field or reconstruct
	string view_sql;
	if (!view_info->sql.empty()) {
		view_sql = view_info->sql;
	} else if (view_info->query) {
		view_sql = view_info->query->ToString();
	} else {
		throw InternalException("Cannot commit view '%s': no SQL representation available", view_name);
	}

	// Build the CreateViewRequest JSON body
	JSONWriter writer;
	auto root_object = writer.CreateObject();
	writer.SetRoot(root_object);

	// name
	root_object.AddString("name", view_name.c_str());

	// schema — the view's output column types
	auto schema_obj = writer.CreateObject();
	root_object.Add("schema", schema_obj);
	schema_obj.AddString("type", "struct");
	schema_obj.Add("schema-id", writer.CreateSignedInteger(0));
	auto fields_arr = writer.CreateArray();
	schema_obj.Add("fields", fields_arr);
	idx_t next_field_id = 1;
	auto column_names = view_info->names;
	for (idx_t i = 0; i < view_info->aliases.size(); i++) {
		if (!view_info->aliases[i].empty()) {
			column_names[i] = view_info->aliases[i];
		}
	}
	// Iceberg schemas require unique field names, just like a bound DuckDB view.
	QueryResult::DeduplicateColumns(column_names);
	for (idx_t i = 0; i < view_info->types.size(); i++) {
		auto &col_name = column_names[i];
		auto field = IcebergTypeHelper::CreateIcebergRestType(
		    col_name.GetIdentifierName(), view_info->types[i], false, "", Value(),
		    [&next_field_id]() { return next_field_id++; }, 2);
		fields_arr.Append(field.ToJSON(writer));
	}
	auto identifier_field_ids = writer.CreateArray();
	schema_obj.Add("identifier-field-ids", identifier_field_ids);
	(void)identifier_field_ids;

	// view-version
	auto version_obj = writer.CreateObject();
	root_object.Add("view-version", version_obj);
	version_obj.Add("version-id", writer.CreateSignedInteger(1));
	version_obj.Add("timestamp-ms",
	                writer.CreateUnsignedInteger(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp())));
	version_obj.Add("schema-id", writer.CreateSignedInteger(0));

	// summary
	auto summary_obj = writer.CreateObject();
	version_obj.Add("summary", summary_obj);
	summary_obj.AddString("engine-name", "DuckDB");

	// default-namespace
	auto ns_arr = writer.CreateArray();
	version_obj.Add("default-namespace", ns_arr);
	for (auto &ns_item : schema_entry.namespace_items) {
		ns_arr.AppendString(ns_item);
	}

	// Omit default-catalog: unqualified references belong to this catalog,
	// even when another session attaches it using a different local name.

	// representations
	auto repr_arr = writer.CreateArray();
	version_obj.Add("representations", repr_arr);
	auto repr_obj = writer.CreateObject();
	repr_arr.Append(repr_obj);
	repr_obj.AddString("type", IcebergConstants::ViewSQLRepresentationType);
	repr_obj.AddString("sql", view_sql.c_str());
	repr_obj.AddString("dialect", IcebergConstants::ViewDuckDBDialect);

	// properties (empty)
	root_object.Add("properties", writer.CreateObject());

	return writer.ToString();
}

} // namespace duckdb
