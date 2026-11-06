#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

//! An unsupported view remains visible to catalog inspection and DROP without executable placeholder SQL.
class UnsupportedIcebergViewEntry : public ViewCatalogEntry {
public:
	UnsupportedIcebergViewEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info, string reason);
	const SelectStatement &GetQuery() override;
	void BindView(ClientContext &context, BindViewAction action) override;
	unique_ptr<CreateInfo> GetInfo() const override;
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	string ToSQL() const override;

private:
	string reason;
};

class IcebergSchemaEntry;

//! The CreateViewRequest (see the REST catalog spec) of a view created in 'schema_entry', as JSON. Catalogs without
//! a catalog server build the view's metadata from it.
string CreateIcebergViewRequestJSON(const IcebergSchemaEntry &schema_entry, const CreateViewInfo &view_info);

} // namespace duckdb
