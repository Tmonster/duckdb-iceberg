"""DuckDB inserts into the Hive tables of a Glue catalog, and other engines see the new partitions.

Needs the moto server of 'make glue-moto'; run with:

    make test_glue_catalog_python_release
"""

from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from test_glue_hive_tables import (
    CSV_SERDE,
    GLUE_CONFIG,
    TEXT_FORMATS,
    attach,
    create_hive_table,
    glue,  # noqa: F401 (fixture)
    pytestmark,  # noqa: F401
    region,  # noqa: F401 (fixture)
)


def test_insert_into_hive_tables(unittest_binary, print_unittest_stdin, glue, region, tmp_path):
    hive = tmp_path / "hive"
    warehouse = tmp_path / "warehouse"
    glue.create_database(DatabaseInput={"Name": "sales"})
    create_hive_table(glue, "customers", hive / "customers", [("id", "int"), ("name", "string")])
    create_hive_table(
        glue,
        "orders",
        hive / "orders",
        [("order_id", "int"), ("amount", "double")],
        partition_keys=[("year", "int"), ("region", "string")],
    )
    create_hive_table(
        glue, "notes", hive / "notes", [("id", "int"), ("note", "string")], serde=CSV_SERDE, formats=TEXT_FORMATS
    )

    with DuckDBUnittestRunner(unittest_binary, test_config=GLUE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach(test, region, warehouse)
        test.query("I", "INSERT INTO glue.sales.customers VALUES (1, 'a'), (2, 'b')", [(2,)])
        test.query("I", "INSERT INTO glue.sales.customers (id) VALUES (3)", [(1,)])
        test.query(
            "IT", "SELECT id, name FROM glue.sales.customers ORDER BY id", [(1, "a"), (2, "b"), (3, "NULL")]
        )
        # Every partition the rows fall into is written to <key>=<value> directories and registered in Glue
        test.query(
            "I",
            "INSERT INTO glue.sales.orders VALUES (1, 10.0, 2024, 'eu'), (2, 20.0, 2024, 'us'), (3, 5.0, 2025, 'eu')",
            [(3,)],
        )
        # Appending to an existing partition keeps its registration
        test.query("I", "INSERT INTO glue.sales.orders VALUES (4, 1.0, 2024, 'eu')", [(1,)])
        test.query(
            "IIT",
            "SELECT year, count(*), string_agg(order_id::VARCHAR, ',' ORDER BY order_id) FROM glue.sales.orders "
            "WHERE region = 'eu' GROUP BY year ORDER BY year",
            [(2024, 2, "1,4"), (2025, 1, "3")],
        )
        test.query("I", "INSERT INTO glue.sales.notes VALUES (1, 'first'), (2, 'plain')", [(2,)])
        test.query("IT", "SELECT id, note FROM glue.sales.notes WHERE id = 2", [(2, "plain")])

        # Glue has no transactions: a Hive insert can't share a transaction with Iceberg changes
        test.statement_ok("CREATE TABLE glue.sales.returns (order_id INT)")
        test.statement_ok("BEGIN")
        test.statement_ok("INSERT INTO glue.sales.returns VALUES (1)")
        test.statement_error(
            "INSERT INTO glue.sales.customers VALUES (9, 'x')", "in a transaction that changes Iceberg tables"
        )
        test.statement_ok("ROLLBACK")
        test.statement_ok("BEGIN")
        test.statement_ok("INSERT INTO glue.sales.customers VALUES (10, 'y')")
        test.statement_error(
            "INSERT INTO glue.sales.returns VALUES (2)", "in a transaction that inserted into a Hive table"
        )
        test.statement_ok("ROLLBACK")
        # ... and the Hive insert was published regardless of the ROLLBACK
        test.query("I", "SELECT count(*) FROM glue.sales.customers WHERE id = 10", [(1,)])
        test.query("I", "SELECT count(*) FROM glue.sales.returns", [(0,)])

    # The partitions are registered for every other Glue client
    partitions = glue.get_partitions(DatabaseName="sales", TableName="orders")["Partitions"]
    assert sorted(tuple(partition["Values"]) for partition in partitions) == [
        ("2024", "eu"),
        ("2024", "us"),
        ("2025", "eu"),
    ]
    for partition in partitions:
        year, region_value = partition["Values"]
        assert partition["StorageDescriptor"]["Location"] == str(hive / "orders" / f"year={year}" / f"region={region_value}")
        assert partition["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"].endswith("ParquetHiveSerDe")
    assert any(Path(hive / "orders" / "year=2024" / "region=eu").iterdir())


def test_insert_refuses_unsupported_hive_layouts(unittest_binary, print_unittest_stdin, glue, region, tmp_path):
    hive = tmp_path / "hive"
    glue.create_database(DatabaseInput={"Name": "sales"})
    # A bucketed table: other engines expect one file per bucket, named by its bucket number
    glue.create_table(
        DatabaseName="sales",
        TableInput={
            "Name": "bucketed",
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": str(hive / "bucketed"),
                "Columns": [{"Name": "id", "Type": "int"}],
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
                "BucketColumns": ["id"],
                "NumberOfBuckets": 4,
            },
        },
    )
    # A partitioned table with a partition registered outside the table location
    create_hive_table(glue, "external", hive / "external", [("id", "int")], partition_keys=[("year", "int")])
    glue.batch_create_partition(
        DatabaseName="sales",
        TableName="external",
        PartitionInputList=[{"Values": ["2020"], "StorageDescriptor": {"Location": str(tmp_path / "elsewhere")}}],
    )
    (tmp_path / "elsewhere").mkdir()

    with DuckDBUnittestRunner(unittest_binary, test_config=GLUE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach(test, region, tmp_path / "warehouse")
        test.statement_error("INSERT INTO glue.sales.bucketed VALUES (1)", "it is bucketed")
        # Refused before anything is written, also for rows of other partitions
        test.statement_error("INSERT INTO glue.sales.external VALUES (1, 2024)", "outside the table location")
        test.query("I", "SELECT count(*) FROM glue.sales.external", [(0,)])

    assert not (hive / "bucketed").exists() or not any((hive / "bucketed").iterdir())
    assert not (hive / "external").exists() or not any((hive / "external").rglob("*.parquet"))
