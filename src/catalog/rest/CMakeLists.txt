add_subdirectory(api)
add_subdirectory(storage)
add_subdirectory(transaction)

add_library(iceberg_catalog_rest OBJECT iceberg_rest_catalog.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest>
    PARENT_SCOPE)
