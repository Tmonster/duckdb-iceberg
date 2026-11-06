"""DuckDB and PyIceberg share an Iceberg SQL catalog stored in SQLite.

DuckDB attaches the catalog with CATALOG_TYPE 'sql' and PyIceberg opens it as a SqlCatalog: both read the tables the
other one created, and append to them. Needs no catalog service: the catalog and the warehouse live in pytest's
temporary directory. Run with:

    python3 -m pytest test/python/sql_catalog --unittest-binary ./build/release/test/unittest
"""

from pathlib import Path

import pytest

from duckdb_unittest import REPO_ROOT, DuckDBUnittestRunner

pytest.importorskip("sqlalchemy")
pa = pytest.importorskip("pyarrow")
sql_catalog = pytest.importorskip("pyiceberg.catalog.sql")

SQL_SQLITE_CONFIG = REPO_ROOT / "test" / "configs" / "sql_sqlite.json"
CATALOG_NAME = "interop"


@pytest.fixture()
def shared_catalog(tmp_path: Path):
    """The SQLite database and the warehouse of a catalog shared by DuckDB and PyIceberg"""
    database = tmp_path / "catalog.db"
    warehouse = tmp_path / "warehouse"
    return database, warehouse


def pyiceberg_catalog(database: Path, warehouse: Path):
    return sql_catalog.SqlCatalog(CATALOG_NAME, uri=f"sqlite:///{database}", warehouse=f"file://{warehouse}")


def attach_shared_catalog(test: DuckDBUnittestRunner, database: Path, warehouse: Path) -> None:
    test.statement_ok(
        f"""
        ATTACH '{warehouse}' AS {CATALOG_NAME} (
            TYPE ICEBERG, CATALOG_TYPE 'sql', URI 'sqlite:///{database}', CATALOG_NAME '{CATALOG_NAME}'
        )
        """
    )


def test_duckdb_and_pyiceberg_share_a_sql_catalog(unittest_binary, print_unittest_stdin, shared_catalog):
    database, warehouse = shared_catalog

    # DuckDB creates a namespace and a table
    with DuckDBUnittestRunner(unittest_binary, test_config=SQL_SQLITE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach_shared_catalog(test, database, warehouse)
        test.statement_ok(f"CREATE SCHEMA {CATALOG_NAME}.ns")
        test.statement_ok(
            f"CREATE TABLE {CATALOG_NAME}.ns.from_duckdb AS SELECT range::INT AS id, 'v' || range AS val FROM range(5)"
        )

    # PyIceberg sees them, reads the table and appends to it, and creates a table of its own
    catalog = pyiceberg_catalog(database, warehouse)
    assert catalog.list_namespaces() == [("ns",)]
    assert catalog.list_tables("ns") == [("ns", "from_duckdb")]
    from_duckdb = catalog.load_table("ns.from_duckdb")
    assert from_duckdb.scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [0, 1, 2, 3, 4],
        "val": ["v0", "v1", "v2", "v3", "v4"],
    }
    from_duckdb.append(pa.table({"id": pa.array([100], pa.int32()), "val": ["from pyiceberg"]}))

    from_pyiceberg = catalog.create_table(
        "ns.from_pyiceberg", schema=pa.schema([("id", pa.int64()), ("name", pa.string())])
    )
    from_pyiceberg.append(pa.table({"id": [1, 2], "name": ["a", "b"]}))

    # DuckDB reads both, including PyIceberg's commits, and appends to PyIceberg's table
    with DuckDBUnittestRunner(unittest_binary, test_config=SQL_SQLITE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach_shared_catalog(test, database, warehouse)
        test.query("II", f"SELECT count(*), sum(id) FROM {CATALOG_NAME}.ns.from_duckdb", [(6, 110)])
        test.query("IT", f"SELECT id, name FROM {CATALOG_NAME}.ns.from_pyiceberg ORDER BY id", [(1, "a"), (2, "b")])
        test.statement_ok(f"INSERT INTO {CATALOG_NAME}.ns.from_pyiceberg VALUES (3, 'c')")

    # PyIceberg reads DuckDB's commit to its table
    catalog = pyiceberg_catalog(database, warehouse)
    assert catalog.load_table("ns.from_pyiceberg").scan().to_arrow().sort_by("id").to_pydict() == {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
    }


def test_pyiceberg_sees_duckdb_renames_and_drops(unittest_binary, print_unittest_stdin, shared_catalog):
    database, warehouse = shared_catalog

    with DuckDBUnittestRunner(unittest_binary, test_config=SQL_SQLITE_CONFIG, print_stdin=print_unittest_stdin) as test:
        attach_shared_catalog(test, database, warehouse)
        test.statement_ok(f"CREATE SCHEMA {CATALOG_NAME}.ns")
        test.statement_ok(f"CREATE TABLE {CATALOG_NAME}.ns.renamed_by_duckdb AS SELECT 1 AS i")
        test.statement_ok(f"CREATE TABLE {CATALOG_NAME}.ns.dropped_by_duckdb AS SELECT 2 AS i")
        test.statement_ok(f"ALTER TABLE {CATALOG_NAME}.ns.renamed_by_duckdb RENAME TO renamed")
        test.statement_ok(f"DROP TABLE {CATALOG_NAME}.ns.dropped_by_duckdb")

    catalog = pyiceberg_catalog(database, warehouse)
    assert catalog.list_tables("ns") == [("ns", "renamed")]
    assert catalog.load_table("ns.renamed").scan().to_arrow().to_pydict() == {"i": [1]}
