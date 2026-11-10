add_subdirectory(hive)

add_library(iceberg_catalog_glue OBJECT iceberg_glue_catalog.cpp iceberg_glue_client.cpp iceberg_glue_commit.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_glue>
    PARENT_SCOPE)
