add_library(iceberg_catalog_metastore OBJECT iceberg_metastore_catalog.cpp iceberg_metastore_commit.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_metastore>
    PARENT_SCOPE)
