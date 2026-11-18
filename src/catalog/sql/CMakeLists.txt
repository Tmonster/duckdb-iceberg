add_library(iceberg_catalog_sql OBJECT iceberg_sql_catalog.cpp iceberg_sql_commit.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_sql>
    PARENT_SCOPE)
