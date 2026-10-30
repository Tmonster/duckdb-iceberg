> [!WARNING]
> This extension is experimental. APIs and behavior may change, and some catalog-specific features are not yet supported.

# DuckDB extension for Apache Iceberg

This repository contains DuckDB's Apache Iceberg extension. It adds support for reading and writing Iceberg tables, inspecting Iceberg metadata, and attaching Iceberg REST catalogs.

User-facing documentation is available on the [Iceberg extension page](https://duckdb.org/docs/extensions/iceberg).

## Row lineage inside transactions

For format version 3 tables, rows inserted within a transaction have NULL `_row_id`
and `_last_updated_sequence_number` until commit. An UPDATE preserves an already
assigned `_row_id`, including through repeated updates in the same transaction,
but its new `_last_updated_sequence_number` remains NULL until commit. Rows from
before a v2-to-v3 upgrade also retain NULL lineage until a v3 snapshot assigns it.

Each data-changing statement publishes a snapshot at commit. A row inserted and
then updated within one transaction can receive a different `_row_id` in each
of those snapshots: the updates cannot preserve an ID that has not been assigned
yet. After commit, further updates preserve its assigned ID. Use a business key
to identify pending rows, rather than relying on their nullable lineage columns.

## SQL catalogs

Besides Iceberg REST catalogs, the extension can attach a catalog that is stored in the tables of a SQL
database, compatible with the [JDBC catalog](https://iceberg.apache.org/docs/latest/jdbc/) of Iceberg Java
and the SQL catalog of PyIceberg. There is no catalog server: DuckDB writes the table metadata files itself,
and stores a pointer to each table's current metadata file in the `iceberg_tables` table.

```sql
ATTACH 's3://bucket/warehouse' AS lake (
    TYPE ICEBERG,
    CATALOG_TYPE 'sql',
    URI 'duckdb:/path/to/catalog.duckdb'
);
```

- The ATTACH path is the warehouse: new tables are created at `<warehouse>/<namespace>/<table>`, or below the
  namespace's `location` property.
- `URI` is the database holding the catalog. `duckdb:<path>` is a DuckDB database file. SQLAlchemy and JDBC
  URIs (`sqlite:///<path>`, `postgresql://...`, `jdbc:sqlite:<path>`, ...) use the DuckDB `sqlite` and
  `postgres` extensions. SQLite databases are switched to WAL mode, so that readers don't block commits.
- `CATALOG_NAME` is the value of the `catalog_name` column, which lets several catalogs share a database. It
  defaults to the ATTACH alias, so attach the same catalog under another alias with `CATALOG_NAME`.
- `INIT_CATALOG_TABLES` (default `true`) creates the catalog tables if they don't exist.
- All schema and table changes of a DuckDB transaction are committed in one transaction of the catalog
  database. Concurrent commits to the same table are detected and retried as configured by the table's
  `commit.retry.*` properties.
- Dropping a table only unregisters it, unless the catalog is attached with `PURGE_REQUESTED true`, which
  removes the table's files if they are inside the warehouse.
- Views are stored as `VIEW` rows of `iceberg_tables`, pointing to Iceberg view metadata files, as in the JDBC
  catalog. Catalogs that still have the JDBC catalog's original schema, without the `iceberg_type` column,
  don't support views. PyIceberg versions without apache/iceberg-python#3709 list these views as tables.

## Development setup

Clone the repository with its DuckDB and extension tooling submodules:

```shell
git clone --recurse-submodules https://github.com/duckdb/duckdb-iceberg.git
cd duckdb-iceberg
```

If the repository is already cloned, initialize the submodules with:

```shell
git submodule update --init --recursive
```

### Prerequisites

Building requires a C++ toolchain, CMake, and [vcpkg](https://vcpkg.io/en/getting-started.html). The repository supplies its own vcpkg manifest and ports; pass the vcpkg toolchain file when building.

Catalog-backed tests and test-data generation have additional requirements:

- Python 3 and the packages in `scripts/requirements.txt`
- Java compatible with the selected PySpark runtime
- Docker and Docker Compose for the local REST catalogs
- Git for catalog repositories cloned under `.catalogs/`

The catalog data targets create `.venv-spark4` and install the pinned Python requirements automatically. `make install_requirements` installs those requirements into the currently active Python environment instead.

## Building

Set the vcpkg toolchain path for each invocation or export it in your shell:

```shell
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make debug
```

Common build targets are:

```shell
make debug       # Debug build with assertions and sanitizers
make release     # Optimized release build; also the default `make` target
make reldebug    # Optimized build with debug information
make relassert   # Optimized build with assertions
```

For a release build, the resulting DuckDB shell and loadable extension are:

```text
build/release/duckdb
build/release/extension/iceberg/iceberg.duckdb_extension
```

The top-level `Makefile` delegates standard build, test, and formatting targets to `extension-ci-tools/makefiles/duckdb_extension.Makefile`. It builds Iceberg together with the required test extensions configured in `extension_config.cmake`.

## Running tests

Build the matching configuration before running its tests. The default test target uses the release test binary:

> [!CAUTION]
> Tests are not assumed to be isolated from one another. Catalog-backed tests in particular can mutate shared catalogs, warehouses, schemas, tables, and generated files. Do not run test files or suites concurrently unless you have verified that they use independent resources and cannot affect each other. This includes separate unittest processes, pytest workers such as `pytest-xdist`, and overlapping local or CI jobs.

```shell
make release
make test
```

Other configurations can be tested explicitly:

```shell
make debug
make test_debug

make reldebug
make test_reldebug
```

The SQL catalog suites run the catalog-agnostic tests against a SQL catalog stored in a DuckDB file or a SQLite
database, which need no service, or in PostgreSQL. The PostgreSQL suite needs a build that includes the
`postgres_scanner` extension, whose client library is built with `bison` and `flex`:

```shell
make test_sql_catalog_release
make test_sql_catalog_sqlite_release

ICEBERG_SQL_CATALOG_POSTGRES=1 make release
make sql-postgres
make test_sql_catalog_postgres_release
make sql-postgres-stop
```

`make test_sql_catalog_python_release` checks that DuckDB and PyIceberg can share a SQL catalog; it needs
`pyiceberg[sql-sqlite,pyarrow]`.

Use `T` to narrow a Make-based run, or invoke the sqllogictest runner directly:

```shell
make test_debug T=test/sql/local/iceberg_scans/

./build/debug/test/unittest \
  test/sql/local/iceberg_scans/iceberg_scan.test
```

Run formatting checks and fixes with:

```shell
make format-check
make format-fix
```

## Local REST catalogs

The catalog targets start local services, generate compatible Iceberg test data, and record the selected catalog in `.catalogs/.active_catalog`.

| Catalog | Start services | Start and generate data | Notes |
| --- | --- | --- | --- |
| Apache Iceberg REST fixture | `make fixture` | `make fixture-data` | Uses the Compose setup in `scripts/` |
| Apache Iceberg REST fixture (latest) | `make fixture-latest` | `make fixture-latest-data` | Tracks `apache/iceberg-rest-fixture:latest` in a separate compatibility lane |
| Apache Gravitino | `make gravitino` | `make gravitino-data` | Uses the standalone Gravitino REST server and SeaweedFS Compose setup in `scripts/gravitino/`; may add `seaweedfs` to `/etc/hosts` with `sudo` |
| Lakekeeper | `make lakekeeper` | `make lakekeeper-data` | Clones a pinned revision, applies the repository patch, and may add `seaweedfs` to `/etc/hosts` with `sudo` |
| Nessie | `make nessie` | `make nessie-data` | Uses Nessie's `catalog-auth-s3` Compose setup |
| Apache Polaris | `make polaris` | `make polaris-data` | Clones the `release/1.4.x` branch and uses its RustFS quickstart |
| Test-only v2/v3 mock | `make mock` | Not required | Python standard library and local warehouse; [SQL testing and skip policy](scripts/mock_rest_catalog/README.md) |

Starting a catalog stops the catalog currently named in `.catalogs/.active_catalog`. To stop one explicitly, use `make <catalog>-stop`. Catalog clones, runtime state, and generated data are kept in ignored directories.

To generate only the data needed by one registered generator, pass a pytest `-k` expression through `TEST`:

```shell
TEST=all_types_table make fixture-data
```

The fixture also supports generation for local file-based tests:

```shell
make fixture-data-local
```

### Catalog-backed sqllogictests

Each REST catalog has a config in `test/configs/`. A config initializes credentials and the `my_datalake` attachment, statically loads required extensions, sets `CATALOG_TEST_CONFIG_SETUP`, and lists catalog-specific skips where necessary.

After starting a catalog, resolve its config from `.catalogs/.active_catalog` by running the helper directly:

```shell
./build/debug/test/unittest --order lex \
  "$PWD/test/sql/local/catalog_test_config_setup/*" \
  --test-config "$(scripts/catalog_test_config.sh)"
```

To run one file:

```shell
./build/debug/test/unittest \
  --test-config "$(scripts/catalog_test_config.sh)" \
  test/sql/local/catalog_test_config_setup/catalog_agnostic/create/test_create_table.test
```

`scripts/catalog_test_config.sh` accepts `fixture`, `fixture-latest`, `gravitino`, `lakekeeper`, `nessie`, `polaris`, or `mock`. It reports an error for a missing, empty, local-only, or unknown active-catalog marker.

For the mock, run the catalog-agnostic SQL suite excluding `.test_slow`, with capability flags and skips controlled by `test/configs/mock.json`. You can pass that config directly after `make mock`; see the [mock catalog instructions](scripts/mock_rest_catalog/README.md). It has no Spark generator or Python integration profile. Stop it with `make mock-stop`.

### Catalog-backed Python tests

The Python integration tests also use the active catalog and require a built unittest binary:

```shell
python3 -m pytest -vv test/python \
  --unittest-binary ./build/debug/test/unittest \
  --test-python-verbosity verbose
```

Verbose mode prints the resolved catalog and Spark runtime and explains capability-based skips. Cloud-only tests under `test/python/cloud/` are excluded unless that directory is requested explicitly.

## Test data generation

The generator under `scripts/data_generators/` uses PySpark and Iceberg to populate the active catalog. Generator cases live under `scripts/data_generators/tests/`, and intermediate results are written below `data/generated/intermediates/`.

See [scripts/data_generators/README.md](scripts/data_generators/README.md) for instructions on adding generator cases, catalog connections, skips, and expected failures.

## Acknowledgements

This extension was initially developed as part of a customer project for [RelationalAI](https://relational.ai/), who agreed to open source it. We thank RelationalAI for supporting open source development and enabling us to share the extension with the community.
