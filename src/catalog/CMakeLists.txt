add_subdirectory(catalog_entry)
add_subdirectory(rest)
add_subdirectory(transaction)

add_library(iceberg_catalog OBJECT iceberg_catalog.cpp iceberg_schema_set.cpp
                                   iceberg_table_set.cpp iceberg_view_entry.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog>
    PARENT_SCOPE)
