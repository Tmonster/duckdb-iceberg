PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='parquet;httpfs'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-rest-catalog: install_requirements
	./scripts/start-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# make data_ci is used in CI. Calling data clean fails
# with permission errors since the docker image running the spark rest API
# is mounted to data/generated/iceberg/spark-rest
data_ci:
	mkdir -p data
	python3 scripts/data_generators/generate_data.py

# Custom makefile targets
data: data_clean
	mkdir -p data
	python3 scripts/data_generators/generate_data.py

data_large: data data_clean
	python3 scripts/data_generators/generate_data.py

data_clean:
	rm -rf data/generated
