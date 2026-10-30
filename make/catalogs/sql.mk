# Iceberg SQL catalogs (CATALOG_TYPE 'sql'). The DuckDB-file and SQLite configs need no service; the PostgreSQL
# config needs 'make sql-postgres' and a build with ICEBERG_SQL_CATALOG_POSTGRES=1 (see extension_config.cmake).
# None of them consult or change the active service catalog.
SQL_CATALOG_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
SQL_CATALOG_CONFIG ?= sql.json
SQL_CATALOG_CONFIGURATIONS := release debug reldebug relassert
SQL_CATALOG_POSTGRES_CONTAINER ?= iceberg-sql-catalog-postgres
SQL_CATALOG_POSTGRES_PORT ?= 55432
SQL_CATALOG_POSTGRES_IMAGE ?= postgres:17

.PHONY: test_sql_catalog sql-postgres sql-postgres-stop \
	$(addprefix test_sql_catalog_,$(SQL_CATALOG_CONFIGURATIONS)) \
	$(addprefix test_sql_catalog_sqlite_,$(SQL_CATALOG_CONFIGURATIONS)) \
	$(addprefix test_sql_catalog_postgres_,$(SQL_CATALOG_CONFIGURATIONS))

# A SQL catalog in a DuckDB database file
$(addprefix test_sql_catalog_,$(SQL_CATALOG_CONFIGURATIONS)):
	$(MAKE) test_sql_catalog SQL_CATALOG_CONFIG=sql.json \
		SQL_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_sql_catalog_%,%,$@)/test/unittest"

# A SQL catalog in a SQLite database
$(addprefix test_sql_catalog_sqlite_,$(SQL_CATALOG_CONFIGURATIONS)):
	$(MAKE) test_sql_catalog SQL_CATALOG_CONFIG=sql_sqlite.json \
		SQL_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_sql_catalog_sqlite_%,%,$@)/test/unittest"

# A SQL catalog in PostgreSQL, started with 'make sql-postgres'
$(addprefix test_sql_catalog_postgres_,$(SQL_CATALOG_CONFIGURATIONS)):
	$(MAKE) test_sql_catalog SQL_CATALOG_CONFIG=sql_postgres.json \
		SQL_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_sql_catalog_postgres_%,%,$@)/test/unittest"

test_sql_catalog:
	"$(SQL_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/$(SQL_CATALOG_CONFIG)" \
		"test/sql/local/catalog_test_config_setup/catalog_specifc/sql/*" && \
	"$(SQL_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/$(SQL_CATALOG_CONFIG)" \
		"$(SQL_CATALOG_TEST_FILTER)" "exclude:*.test_slow"

# DuckDB and PyIceberg sharing a SQL catalog in SQLite; needs pyiceberg[sql-sqlite,pyarrow]
.PHONY: $(addprefix test_sql_catalog_python_,$(SQL_CATALOG_CONFIGURATIONS))
$(addprefix test_sql_catalog_python_,$(SQL_CATALOG_CONFIGURATIONS)):
	python3 -m pytest -vv test/python/sql_catalog \
		--unittest-binary "$(PROJ_DIR)build/$(patsubst test_sql_catalog_python_%,%,$@)/test/unittest"

# A PostgreSQL server for test/configs/sql_postgres.json, with an empty 'iceberg_catalog' database
sql-postgres: sql-postgres-stop
	docker run -d --name $(SQL_CATALOG_POSTGRES_CONTAINER) -p 127.0.0.1:$(SQL_CATALOG_POSTGRES_PORT):5432 \
		-e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=iceberg_catalog $(SQL_CATALOG_POSTGRES_IMAGE)
	@for i in $$(seq 1 60); do \
		docker exec $(SQL_CATALOG_POSTGRES_CONTAINER) pg_isready -U postgres -d iceberg_catalog >/dev/null 2>&1 && exit 0; \
		sleep 1; \
	done; echo "PostgreSQL did not become ready" >&2; exit 1

sql-postgres-stop:
	-docker rm -f $(SQL_CATALOG_POSTGRES_CONTAINER) >/dev/null 2>&1
