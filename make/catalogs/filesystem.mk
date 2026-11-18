# The Iceberg filesystem catalog (CATALOG_TYPE 'filesystem') needs no service.
# It neither consults nor changes the active service catalog.
FILESYSTEM_CATALOG_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
FILESYSTEM_CATALOG_CONFIGURATIONS := release debug reldebug relassert
.PHONY: test_filesystem_catalog $(addprefix test_filesystem_catalog_,$(FILESYSTEM_CATALOG_CONFIGURATIONS))
$(addprefix test_filesystem_catalog_,$(FILESYSTEM_CATALOG_CONFIGURATIONS)):
	$(MAKE) test_filesystem_catalog \
		FILESYSTEM_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_filesystem_catalog_%,%,$@)/test/unittest"

test_filesystem_catalog:
	"$(FILESYSTEM_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/filesystem.json" \
		"test/sql/local/catalog_test_config_setup/catalog_specifc/filesystem/*" && \
	"$(FILESYSTEM_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/filesystem.json" \
		"$(FILESYSTEM_CATALOG_TEST_FILTER)" "exclude:*.test_slow"
