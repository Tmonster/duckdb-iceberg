# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
)

duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 624deb85df218c0c0f84f558ad38a1ca0b67de1d
)

duckdb_extension_load(avro
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 0d7af391bd0aa201b2bdcfb994b7a575ad810155
)

if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG 880da03202acc973d6ee7f3a0423dae5a6dea83b
            APPLY_PATCHES
    )
endif ()
endif()

duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG cb5b2825eff68fc91f47e917ba88bf2ed84c2dd3
        INCLUDE_DIR extension/httpfs/include
        APPLY_PATCHES
)
