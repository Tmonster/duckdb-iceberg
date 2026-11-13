# This file is included by DuckDB's build system. It specifies which extension to load
if (NOT EMSCRIPTEN)
  duckdb_extension_load(avro
  LOAD_TESTS
  GIT_URL https://github.com/duckdb/duckdb-avro
  GIT_TAG b7b157f50dd0a3e6cfd30d9dd273a388fc2b9939
  SUBMODULES "third_party/avro-c"
  APPLY_PATCHES
)
  duckdb_extension_statically_link(avro)
endif()

# Extension from this repo
duckdb_extension_load(parquet)
duckdb_extension_load(json)
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# Static registration order is also database startup load order. Avro above and
# Parquet here must precede Iceberg, so fresh databases (including CSV result readers)
# do not try to auto-install unsigned copies from the build's extension repository.
duckdb_extension_statically_link(parquet json iceberg)

if (NOT EMSCRIPTEN)
  # Use DuckDB's pinned HTTPFS configuration without relying on Make build flags.
  include("${EXTENSION_CONFIG_BASE_DIR}/httpfs.cmake")
  duckdb_extension_load(tpch)
  duckdb_extension_load(icu)
  duckdb_extension_statically_link(tpch icu httpfs)
  #  duckdb_extension_load(ducklake
  #        LOAD_TESTS
  #        GIT_URL https://github.com/duckdb/ducklake
  #        GIT_TAG 417a9117f590e8141742ac0579d02f6a90162e37
  #)

  # Databases that can hold an Iceberg SQL catalog (CATALOG_TYPE 'sql'), for the SQL catalog tests.
  # Pinned to the versions of DuckDB's extension configs, see duckdb/.github/config/extensions/.
  duckdb_extension_load(sqlite_scanner
          GIT_URL https://github.com/duckdb/duckdb-sqlite
          GIT_TAG e223dc01493414cef6d5960744f1e048d59fc592
          SUBMODULES database-connector
  )
  # The PostgreSQL client library makes this a slower build, only needed to test SQL catalogs in PostgreSQL
  if (DEFINED ENV{ICEBERG_SQL_CATALOG_POSTGRES} AND NOT MINGW)
    duckdb_extension_load(postgres_scanner
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG c0f5a3c0c4b68b02afbf669d36d7f0e503985c88
            APPLY_PATCHES
            SUBMODULES database-connector
    )
  endif()

  if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 16ae9c135300f750478d648c2496f2952a6f2827
    )
    duckdb_extension_statically_link(aws)
  endif()
endif()

include("${CMAKE_CURRENT_LIST_DIR}/test/native_catalog/extension_config.cmake")
