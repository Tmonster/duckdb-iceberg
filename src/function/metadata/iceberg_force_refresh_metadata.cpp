#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/main/client_context.hpp"

#include "function/iceberg_functions.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"

namespace duckdb {

namespace {

struct RefreshRow {
	string schema_name;
	string table_name;
	bool refreshed = false;
	string error;
};

struct IcebergForceRefreshBindData : public TableFunctionData {
	//! The name rather than the catalog itself: bind data is handed back as const, and the refresh
	//! needs a mutable catalog. Re-resolving by name in Init keeps that out of the bind data.
	string catalog_name;
};

struct IcebergForceRefreshGlobalState : public GlobalTableFunctionState {
public:
	vector<RefreshRow> rows;
	idx_t offset = 0;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
};

unique_ptr<FunctionData> IcebergForceRefreshBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<IcebergForceRefreshBindData>();

	auto catalog_name = input.inputs[0].ToString();
	auto catalog = Catalog::GetCatalogEntry(context, Identifier(catalog_name));
	if (!catalog) {
		throw InvalidInputException("No database named '%s' is attached", catalog_name);
	}
	if (catalog->GetCatalogType() != "iceberg") {
		throw InvalidInputException("'%s' is a '%s' database; iceberg_force_refresh_metadata only works on an "
		                            "Iceberg catalog",
		                            catalog_name, catalog->GetCatalogType());
	}
	result->catalog_name = catalog_name;

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("refreshed");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("error");
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> IcebergForceRefreshGlobalState::Init(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto result = make_uniq<IcebergForceRefreshGlobalState>();
	auto &bind_data = input.bind_data->Cast<IcebergForceRefreshBindData>();
	auto &ic_catalog = Catalog::GetCatalog(context, Identifier(bind_data.catalog_name)).Cast<IcebergCatalog>();

	//! Both listings short-circuit once they have run for this transaction. Clearing the markers is what
	//! makes this a refresh rather than a no-op when something in the same transaction already listed.
	auto &transaction = IcebergTransaction::Get(context, ic_catalog);
	transaction.called_list_schemas = false;
	transaction.listed_schemas.clear();

	for (auto &schema_entry : ic_catalog.GetSchemas().GetEntries(context)) {
		vector<IcebergTableRefreshResult> table_results;
		schema_entry->tables.RefreshAll(context, table_results);
		for (auto &table_result : table_results) {
			RefreshRow row;
			row.schema_name = schema_entry->name.GetIdentifierName();
			row.table_name = std::move(table_result.table_name);
			row.refreshed = table_result.refreshed;
			row.error = std::move(table_result.error);
			result->rows.push_back(std::move(row));
		}
	}
	return std::move(result);
}

void IcebergForceRefreshFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<IcebergForceRefreshGlobalState>();

	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		output.data[0].SetValue(count, Value(row.schema_name));
		output.data[1].SetValue(count, Value(row.table_name));
		output.data[2].SetValue(count, Value::BOOLEAN(row.refreshed));
		output.data[3].SetValue(count, row.error.empty() ? Value(LogicalType::VARCHAR) : Value(row.error));
		count++;
	}
	output.SetChildCardinality(count);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergForceRefreshMetadataFunction() {
	TableFunctionSet function_set("iceberg_force_refresh_metadata");
	function_set.AddFunction(TableFunction({LogicalType::VARCHAR}, IcebergForceRefreshFunction, IcebergForceRefreshBind,
	                                       IcebergForceRefreshGlobalState::Init));
	return function_set;
}

} // namespace duckdb
