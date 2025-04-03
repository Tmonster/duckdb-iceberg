PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-rest-catalog: install_requirements
	./scripts/start-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# Custom makefile targets
data: data_clean start-rest-catalog
	python3 scripts/data_generators/generate_data.py

data_large: data data_clean
	python3 scripts/data_generators/generate_data.py

# setup polaris server. See PolarisTesting.yml to see instructions for a specific machine.
setup_polaris:
	mkdir polaris_catalog
	git clone https://github.com/apache/polaris.git polaris_catalog
	cd polaris_catalog && ./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true --no-build-cache
	cd polaris_catalog && ./gradlew --stop
	cd polaris_catalog && nohup ./gradlew run > polaris-server.log 2> polaris-error.log &

data_clean:
	rm -rf data/generated

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

format-check:
	python3 scripts/format.py --all --check

format-head:
	python3 scripts/format.py HEAD --fix --noconfirm

format-changes:
	python3 scripts/format.py HEAD --fix --noconfirm

format-main:
	python3 scripts/format.py main --fix --noconfirm

format-check-silent:
	python3 scripts/format.py --all --check --silent
