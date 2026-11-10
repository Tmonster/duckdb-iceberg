"""DuckDB reads the Hive tables of a Glue catalog next to its Iceberg tables.

boto3 registers Hive tables in moto's Glue API (as Athena, Spark or a Glue crawler would), over data files that DuckDB
writes with COPY. DuckDB attaches the catalog with CATALOG_TYPE 'glue' and reads them. Needs the moto server of
'make glue-moto'; run with:

    make test_glue_catalog_python_release
"""

import socket
import uuid
from pathlib import Path

import pytest

from duckdb_unittest import REPO_ROOT, DuckDBUnittestRunner

boto3 = pytest.importorskip("boto3")

GLUE_CONFIG = REPO_ROOT / "test" / "configs" / "glue.json"
MOTO_HOST = "127.0.0.1"
MOTO_PORT = 5055

PARQUET_SERDE = {
    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
}
PARQUET_FORMATS = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
}
CSV_SERDE = {
    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    "Parameters": {"field.delim": "|"},
}
TEXT_FORMATS = {
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
}


def moto_is_running() -> bool:
    try:
        with socket.create_connection((MOTO_HOST, MOTO_PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not moto_is_running(), reason="needs the moto server of 'make glue-moto'")


@pytest.fixture()
def region():
    # moto keeps regions apart: a region of its own gives every test an empty Glue catalog
    return f"hive-test-{uuid.uuid4()}"


@pytest.fixture()
def glue(region):
    return boto3.client(
        "glue",
        region_name=region,
        endpoint_url=f"http://{MOTO_HOST}:{MOTO_PORT}",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def attach(test: DuckDBUnittestRunner, region: str, warehouse: Path) -> None:
    test.statement_ok(
        f"""
        ATTACH ':' AS glue (
            TYPE ICEBERG, CATALOG_TYPE 'glue', ENDPOINT 'http://{MOTO_HOST}:{MOTO_PORT}', REGION '{region}',
            SECRET 'glue_test_secret', DEFAULT_LOCATION '{warehouse}'
        )
        """
    )


def create_hive_table(glue, name, location, columns, partition_keys=(), serde=PARQUET_SERDE, formats=PARQUET_FORMATS):
    glue.create_table(
        DatabaseName="sales",
        TableInput={
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
            "StorageDescriptor": {
                "Location": str(location),
                "Columns": [{"Name": column, "Type": column_type} for column, column_type in columns],
                "SerdeInfo": serde,
                **formats,
            },
            "PartitionKeys": [{"Name": key, "Type": key_type} for key, key_type in partition_keys],
        },
    )


def test_hive_tables_next_to_iceberg_tables(unittest_binary, print_unittest_stdin, glue, region, tmp_path):
    hive = tmp_path / "hive"
    warehouse = tmp_path / "warehouse"
    glue.create_database(DatabaseInput={"Name": "sales"})

    # The data files, as a Hive writer lays them out: an unpartitioned table, a table partitioned by year with one
    # partition outside the table location, and a '|' delimited text table
    with DuckDBUnittestRunner(unittest_binary, test_config=GLUE_CONFIG, print_stdin=print_unittest_stdin) as test:
        (hive / "customers").mkdir(parents=True)
        (hive / "orders" / "year=2023").mkdir(parents=True)
        (hive / "orders" / "year=2024").mkdir(parents=True)
        (hive / "elsewhere").mkdir(parents=True)
        (hive / "notes").mkdir(parents=True)
        test.statement_ok(
            f"COPY (SELECT range::INT AS id, 'c' || range AS name FROM range(3)) "
            f"TO '{hive}/customers/part-0.parquet'"
        )
        test.statement_ok(
            f"COPY (SELECT 1 AS order_id, 0 AS customer_id, 10.5 AS amount) TO '{hive}/orders/year=2023/a.parquet'"
        )
        test.statement_ok(
            f"COPY (SELECT 2 AS order_id, 1 AS customer_id, 20.0 AS amount UNION ALL SELECT 3, 1, 5.0) "
            f"TO '{hive}/orders/year=2024/a.parquet'"
        )
        test.statement_ok(
            f"COPY (SELECT 4 AS order_id, 2 AS customer_id, 1.0 AS amount) TO '{hive}/elsewhere/b.parquet'"
        )
        test.statement_ok(
            f"COPY (SELECT 1 AS id, 'first' AS note UNION ALL SELECT 2, 'second') "
            f"TO '{hive}/notes/notes.txt' (FORMAT csv, DELIMITER '|', HEADER false)"
        )

    create_hive_table(glue, "customers", hive / "customers", [("id", "int"), ("name", "string")])
    create_hive_table(
        glue,
        "orders",
        hive / "orders",
        [("order_id", "int"), ("customer_id", "int"), ("amount", "double")],
        partition_keys=[("year", "int")],
    )
    glue.batch_create_partition(
        DatabaseName="sales",
        TableName="orders",
        PartitionInputList=[
            {"Values": ["2023"], "StorageDescriptor": {"Location": str(hive / "orders" / "year=2023")}},
            {"Values": ["2024"], "StorageDescriptor": {"Location": str(hive / "orders" / "year=2024")}},
            # A partition's files need not follow the <key>=<value> layout below the table
            {"Values": ["2025"], "StorageDescriptor": {"Location": str(hive / "elsewhere")}},
        ],
    )
    create_hive_table(
        glue, "notes", hive / "notes", [("id", "int"), ("note", "string")], serde=CSV_SERDE, formats=TEXT_FORMATS
    )
    # A Delta table: listed by Glue, but not readable through this catalog
    glue.create_table(
        DatabaseName="sales",
        TableInput={
            "Name": "delta_events",
            "Parameters": {"table_type": "DELTA", "spark.sql.sources.provider": "delta"},
            "StorageDescriptor": {"Location": str(tmp_path / "delta"), "Columns": [{"Name": "x", "Type": "int"}]},
        },
    )

    with DuckDBUnittestRunner(unittest_binary, test_config=GLUE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach(test, region, warehouse)
        # An Iceberg table in the same Glue database
        test.statement_ok("CREATE TABLE glue.sales.returns AS SELECT 2 AS order_id, 'damaged' AS reason")

        test.query(
            "TT",
            "SELECT table_name, tags['table_type'] FROM duckdb_tables() WHERE database_name = 'glue' ORDER BY ALL",
            [("customers", "HIVE"), ("notes", "HIVE"), ("orders", "HIVE"), ("returns", "NULL")],
        )
        test.query("IT", "SELECT id, name FROM glue.sales.customers ORDER BY id", [(0, "c0"), (1, "c1"), (2, "c2")])
        test.query(
            "IIRI",
            "SELECT order_id, customer_id, amount, year FROM glue.sales.orders ORDER BY order_id",
            [(1, 0, 10.5, 2023), (2, 1, 20.0, 2024), (3, 1, 5.0, 2024), (4, 2, 1.0, 2025)],
        )
        # A filter on the partition column only reads the matching partition
        test.query("II", "SELECT count(*), sum(order_id) FROM glue.sales.orders WHERE year = 2024", [(2, 5)])
        test.query("IT", "SELECT id, note FROM glue.sales.notes ORDER BY id", [(1, "first"), (2, "second")])
        # Hive and Iceberg tables in one query
        test.query(
            "TTR",
            """
            SELECT c.name, r.reason, o.amount
            FROM glue.sales.orders o
            JOIN glue.sales.customers c ON c.id = o.customer_id
            JOIN glue.sales.returns r ON r.order_id = o.order_id
            """,
            [("c1", "damaged", 20.0)],
        )
        test.statement_error("SELECT * FROM glue.sales.delta_events", "does not exist")
        test.statement_error("DELETE FROM glue.sales.customers", "Cannot DELETE from a Hive table")
        test.statement_error("UPDATE glue.sales.customers SET name = 'x'", "Cannot UPDATE a Hive table")
        test.statement_error("DROP TABLE glue.sales.customers", "Cannot DROP a Hive table")
