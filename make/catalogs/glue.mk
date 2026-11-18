# Iceberg Glue catalogs (CATALOG_TYPE 'glue') against moto, a local implementation of the Glue API. 'make glue-moto'
# starts moto from a virtualenv in .venv-glue-moto (no Docker), on GLUE_MOTO_PORT. It neither consults nor changes
# the active service catalog.
GLUE_CATALOG_TEST_FILTER ?= test/sql/local/catalog_test_config_setup/catalog_agnostic/*
GLUE_CATALOG_CONFIGURATIONS := release debug reldebug relassert
GLUE_MOTO_VENV ?= $(PROJ_DIR).venv-glue-moto
GLUE_MOTO_VERSION ?= 5.2.3
GLUE_MOTO_PORT ?= 5055
GLUE_MOTO_DIR ?= $(PROJ_DIR).catalogs/glue

.PHONY: test_glue_catalog glue-moto glue-moto-stop $(addprefix test_glue_catalog_,$(GLUE_CATALOG_CONFIGURATIONS))

$(GLUE_MOTO_VENV)/bin/moto_server:
	python3 -m venv "$(GLUE_MOTO_VENV)"
	"$(GLUE_MOTO_VENV)/bin/pip" install -q "moto[server,glue]==$(GLUE_MOTO_VERSION)"

# test/configs/glue.json gives every test its own region, so moto must accept any region name
glue-moto: glue-moto-stop $(GLUE_MOTO_VENV)/bin/moto_server
	mkdir -p "$(GLUE_MOTO_DIR)"
	@if curl -s -o /dev/null http://127.0.0.1:$(GLUE_MOTO_PORT)/; then \
		echo "port $(GLUE_MOTO_PORT) is in use, stop the server on it first" >&2; exit 1; \
	fi
	MOTO_ALLOW_NONEXISTENT_REGION=True nohup "$(GLUE_MOTO_VENV)/bin/moto_server" -H 127.0.0.1 -p $(GLUE_MOTO_PORT) \
		> "$(GLUE_MOTO_DIR)/moto.log" 2>&1 < /dev/null & echo $$! > "$(GLUE_MOTO_DIR)/moto.pid"
	@for i in $$(seq 1 60); do \
		curl -s -o /dev/null http://127.0.0.1:$(GLUE_MOTO_PORT)/ && exit 0; \
		sleep 0.5; \
	done; echo "moto did not start, see $(GLUE_MOTO_DIR)/moto.log" >&2; exit 1

glue-moto-stop:
	-@if [ -f "$(GLUE_MOTO_DIR)/moto.pid" ]; then kill "$$(cat "$(GLUE_MOTO_DIR)/moto.pid")" 2>/dev/null; \
		rm -f "$(GLUE_MOTO_DIR)/moto.pid"; fi

$(addprefix test_glue_catalog_,$(GLUE_CATALOG_CONFIGURATIONS)):
	$(MAKE) test_glue_catalog \
		GLUE_CATALOG_TEST_BINARY="$(PROJ_DIR)build/$(patsubst test_glue_catalog_%,%,$@)/test/unittest"

test_glue_catalog:
	@set -e; \
	trap '$(MAKE) glue-moto-stop' EXIT; \
	$(MAKE) glue-moto; \
	"$(GLUE_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/glue.json" \
		"test/sql/local/catalog_test_config_setup/catalog_specifc/glue/*"; \
	"$(GLUE_CATALOG_TEST_BINARY)" --test-config "$(PROJ_DIR)test/configs/glue.json" \
		"$(GLUE_CATALOG_TEST_FILTER)" "exclude:*.test_slow"
