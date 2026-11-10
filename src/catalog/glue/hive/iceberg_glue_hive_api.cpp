#include "catalog/glue/iceberg_glue_catalog.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

#include "catalog/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/glue/hive/iceberg_glue_hive_insert.hpp"
#include "catalog/glue/hive/iceberg_glue_hive_table.hpp"
#include "catalog/transaction/iceberg_transaction.hpp"

namespace duckdb {

namespace {

//! The page size of GetPartitions, Glue's maximum
constexpr int64_t GLUE_PARTITIONS_PAGE_SIZE = 1000;

string StringMember(JSONValue object, const char *key) {
	if (!object.IsValid() || !object.IsObject()) {
		return string();
	}
	auto value = object.GetMember(key);
	return value.IsValid() && value.IsString() ? value.GetString() : string();
}

unordered_map<string, string> StringMap(JSONValue object, const char *key) {
	unordered_map<string, string> result;
	if (!object.IsValid() || !object.IsObject()) {
		return result;
	}
	auto map = object.GetMember(key);
	if (!map.IsValid() || !map.IsObject()) {
		return result;
	}
	map.IterateObject([&](const string &entry_key, JSONValue value) {
		if (value.IsString()) {
			result.emplace(entry_key, value.GetString());
		}
	});
	return result;
}

vector<GlueColumn> Columns(JSONValue object, const char *key) {
	vector<GlueColumn> result;
	if (!object.IsValid() || !object.IsObject()) {
		return result;
	}
	auto columns = object.GetMember(key);
	if (!columns.IsValid() || !columns.IsArray()) {
		return result;
	}
	columns.IterateArray([&](JSONValue column) {
		GlueColumn glue_column;
		glue_column.name = StringMember(column, "Name");
		glue_column.type = StringMember(column, "Type");
		glue_column.comment = StringMember(column, "Comment");
		result.push_back(std::move(glue_column));
	});
	return result;
}

vector<string> Strings(JSONValue object, const char *key) {
	vector<string> result;
	if (!object.IsValid() || !object.IsObject()) {
		return result;
	}
	auto values = object.GetMember(key);
	if (!values.IsValid() || !values.IsArray()) {
		return result;
	}
	values.IterateArray([&](JSONValue value) {
		if (value.IsString()) {
			result.push_back(value.GetString());
		}
	});
	return result;
}

} // namespace

GlueTableInfo IcebergGlueCatalog::ParseTableInfo(JSONValue table) {
	GlueTableInfo result;
	result.name = StringMember(table, "Name");
	result.database_name = StringMember(table, "DatabaseName");
	result.glue_table_type = StringMember(table, "TableType");
	result.table_type = GlueTableTypeFromString(result.glue_table_type);
	result.view_original_text = StringMember(table, "ViewOriginalText");
	result.view_expanded_text = StringMember(table, "ViewExpandedText");
	result.description = StringMember(table, "Description");
	auto storage_descriptor = table.GetMember("StorageDescriptor");
	if (storage_descriptor.IsValid() && storage_descriptor.IsObject()) {
		result.location = StringMember(storage_descriptor, "Location");
		result.input_format = StringMember(storage_descriptor, "InputFormat");
		result.output_format = StringMember(storage_descriptor, "OutputFormat");
		auto serde = storage_descriptor.GetMember("SerdeInfo");
		result.serde_library = StringMember(serde, "SerializationLibrary");
		result.serde_parameters = StringMap(serde, "Parameters");
		result.columns = Columns(storage_descriptor, "Columns");
		result.bucket_columns = Strings(storage_descriptor, "BucketColumns");
		auto buckets = storage_descriptor.GetMember("NumberOfBuckets");
		if (buckets.IsValid() && buckets.IsNumber()) {
			result.number_of_buckets = NumericCast<int32_t>(buckets.GetSignedInteger());
		}
		auto sort_columns = storage_descriptor.GetMember("SortColumns");
		if (sort_columns.IsValid() && sort_columns.IsArray()) {
			sort_columns.IterateArray([&](JSONValue column) {
				GlueColumn sort_column;
				sort_column.name = StringMember(column, "Column");
				auto order = column.GetMember("SortOrder");
				// Glue's SortOrder is 1 for ascending, 0 for descending
				sort_column.sort_order = order.IsValid() && order.IsNumber() && order.GetSignedInteger() == 0
				                             ? GlueSortOrder::DESCENDING
				                             : GlueSortOrder::ASCENDING;
				result.sort_columns.push_back(std::move(sort_column));
			});
		}
	}
	result.partition_keys = Columns(table, "PartitionKeys");
	result.parameters = StringMap(table, "Parameters");
	return result;
}

vector<GluePartitionInfo> IcebergGlueCatalog::GetPartitions(ClientContext &context, const string &database_name,
                                                            const string &table_name) {
	vector<GluePartitionInfo> result;
	ListPages(
	    context, "GetPartitions", "Partitions",
	    [&](JSONWriter &writer, JSONMutableValue &body) {
		    body.AddString("DatabaseName", database_name);
		    body.AddString("TableName", table_name);
		    // Only the values and the location of a partition are used, the column schema every partition repeats
		    // is a large part of the response
		    body.Add("ExcludeColumnSchema", writer.CreateBoolean(true));
		    body.Add("MaxResults", writer.CreateSignedInteger(GLUE_PARTITIONS_PAGE_SIZE));
	    },
	    [&](JSONValue partition) {
		    GluePartitionInfo info;
		    info.values = Strings(partition, "Values");
		    info.location = StringMember(partition.GetMember("StorageDescriptor"), "Location");
		    result.push_back(std::move(info));
	    });
	return result;
}

void IcebergGlueCatalog::BatchCreatePartitions(ClientContext &context, const string &database_name,
                                               const string &table_name,
                                               const vector<GluePartitionInput> &partitions) {
	if (partitions.empty()) {
		return;
	}
	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	IcebergMetastoreIdentifier identifier;
	identifier.namespace_items.push_back(database_name);
	identifier.name = table_name;
	auto table_response = GetTable(context, identifier);
	if (!table_response) {
		throw CatalogException("Glue table '%s.%s' no longer exists", database_name, table_name);
	}
	auto table_descriptor = table_response->GetRoot().GetMember("Table").GetMember("StorageDescriptor");

	// BatchCreatePartition accepts at most 100 partitions per call
	constexpr idx_t BATCH_SIZE = 100;
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		JSONWriter request;
		auto body = request.CreateObject();
		body.AddString("DatabaseName", database_name);
		body.AddString("TableName", table_name);
		auto inputs = request.CreateArray();
		for (idx_t i = offset; i < MinValue<idx_t>(offset + BATCH_SIZE, partitions.size()); i++) {
			auto &partition = partitions[i];
			auto input = request.CreateObject();
			auto values = request.CreateArray();
			for (auto &value : partition.values) {
				values.AppendString(value);
			}
			input.Add("Values", values);
			auto descriptor = request.CreateObject();
			if (table_descriptor.IsValid() && table_descriptor.IsObject()) {
				table_descriptor.IterateObject([&](const string &key, JSONValue value) {
					if (key != "Location") {
						descriptor.Add(key, request.CreateCopy(value));
					}
				});
			}
			descriptor.AddString("Location", partition.location);
			input.Add("StorageDescriptor", descriptor);
			inputs.Append(input);
		}
		body.Add("PartitionInputList", inputs);
		auto response = client.Call(context, "BatchCreatePartition", request, body);
		auto errors = response->GetRoot().GetMember("Errors");
		if (!errors.IsValid() || !errors.IsArray()) {
			continue;
		}
		errors.IterateArray([&](JSONValue error) {
			auto detail = error.GetMember("ErrorDetail");
			auto code = StringMember(detail, "ErrorCode");
			if (code == "AlreadyExistsException") {
				// appending to an existing partition
				return;
			}
			throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s %s", database_name,
			                  table_name, StringUtil::Join(Strings(error, "PartitionValues"), ", "), code,
			                  StringMember(detail, "ErrorMessage"));
		});
	}
}

optional_ptr<CatalogEntry> IcebergGlueCatalog::LookupOtherFormatTable(ClientContext &context,
                                                                      IcebergSchemaEntry &schema,
                                                                      const string &table_name) {
	if (schema.namespace_items.size() != 1) {
		return nullptr;
	}
	auto &transaction = IcebergTransaction::Get(context, *this);
	auto key = schema.name.GetIdentifierName() + "." + table_name;
	auto existing = transaction.other_format_tables.find(key);
	if (existing != transaction.other_format_tables.end()) {
		return existing->second.get();
	}
	IcebergMetastoreIdentifier identifier;
	identifier.namespace_items = schema.namespace_items;
	identifier.name = table_name;
	auto response = GetTable(context, identifier);
	if (!response) {
		return nullptr;
	}
	auto table = response->GetRoot().GetMember("Table");
	if (IsIcebergTable(table)) {
		return nullptr;
	}
	auto entry = IcebergGlueHiveTable::TryCreate(*this, schema, ParseTableInfo(table));
	if (!entry) {
		return nullptr;
	}
	auto &result = *entry;
	transaction.other_format_tables.emplace(key, std::move(entry));
	return result;
}

void IcebergGlueCatalog::ScanOtherFormatTables(ClientContext &context, IcebergSchemaEntry &schema,
                                               const std::function<void(CatalogEntry &)> &callback) {
	if (schema.namespace_items.size() != 1) {
		return;
	}
	auto &transaction = IcebergTransaction::Get(context, *this);
	auto database_name = schema.namespace_items[0];
	vector<reference<CatalogEntry>> entries;
	try {
		ListPages(
		    context, "GetTables", "TableList",
		    [&](JSONWriter &, JSONMutableValue &body) { body.AddString("DatabaseName", database_name); },
		    [&](JSONValue table) {
			    if (IsIcebergTable(table)) {
				    return;
			    }
			    auto table_info = ParseTableInfo(table);
			    auto key = schema.name.GetIdentifierName() + "." + table_info.name;
			    auto existing = transaction.other_format_tables.find(key);
			    if (existing != transaction.other_format_tables.end()) {
				    entries.push_back(*existing->second);
				    return;
			    }
			    unique_ptr<IcebergGlueHiveTable> entry;
			    try {
				    entry = IcebergGlueHiveTable::TryCreate(*this, schema, std::move(table_info));
			    } catch (std::exception &ex) {
				    // A table whose definition can't be read (e.g. an unsupported column type) must not break listing
				    // the other tables. Looking it up by name reports the error.
				    ErrorData error(ex);
				    DUCKDB_LOG_ERROR(context, "Glue table '%s.%s' is not listed: %s", database_name, key,
				                     error.RawMessage());
				    return;
			    }
			    if (!entry) {
				    return;
			    }
			    entries.push_back(*entry);
			    transaction.other_format_tables.emplace(key, std::move(entry));
		    });
	} catch (IcebergGlueException &ex) {
		if (ex.error_type != "EntityNotFoundException") {
			throw;
		}
	}
	for (auto &entry : entries) {
		callback(entry.get());
	}
}

PhysicalOperator &IcebergGlueCatalog::PlanOtherFormatInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                            LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	auto hive_table = IcebergGlueHiveTable::TryGet(op.table);
	if (!hive_table) {
		return IcebergMetastoreCatalog::PlanOtherFormatInsert(context, planner, op, plan);
	}
	auto &transaction = IcebergTransaction::Get(context, *this);
	if (transaction.HasChanges()) {
		throw NotImplementedException("Cannot INSERT into %s in a transaction that changes Iceberg tables: the Hive "
		                              "insert is published when the statement finishes, and Glue can't commit both "
		                              "together",
		                              hive_table->DescribeForError());
	}
	transaction.wrote_other_format_tables = true;
	return IcebergGlueHiveInsert::PlanInsert(context, planner, op, *hive_table, plan);
}

string IcebergGlueCatalog::DescribeOtherFormatTable(const TableCatalogEntry &table) const {
	auto hive_table = IcebergGlueHiveTable::TryGet(table);
	if (hive_table) {
		return hive_table->DescribeForError();
	}
	return IcebergMetastoreCatalog::DescribeOtherFormatTable(table);
}

} // namespace duckdb
