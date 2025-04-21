# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/Tmonster/duckdb-avro
        GIT_TAG fc4ee6f8a21d0fa64eb7c44cd4ca43dbd0f6fa6f
)

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(tpch)


################## AWS
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG main
    )
endif ()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG main
        INCLUDE_DIR extension/httpfs/include
)
