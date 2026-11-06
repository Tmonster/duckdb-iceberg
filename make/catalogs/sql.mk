# The Iceberg SQL catalog (CATALOG_TYPE 'sql') backed by a DuckDB database file needs no service.
# It neither consults nor changes the active service catalog.
SQL_CATALOG_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
.PHONY: test_sql_catalog test_sql_catalog_release test_sql_catalog_debug test_sql_catalog_reldebug test_sql_catalog_relassert
test_sql_catalog_release test_sql_catalog_debug test_sql_catalog_reldebug test_sql_catalog_relassert:
	$(MAKE) test_sql_catalog SQL_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_sql_catalog_%,%,$@)/test/unittest"

test_sql_catalog:
	"$(SQL_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/sql.json" \
		"test/sql/local/catalog_test_config_setup/catalog_specifc/sql/*" && \
	"$(SQL_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/sql.json" \
		"$(SQL_CATALOG_TEST_FILTER)" "exclude:*.test_slow"
