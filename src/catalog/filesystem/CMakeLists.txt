add_library(iceberg_catalog_filesystem OBJECT iceberg_filesystem_catalog.cpp
                                              iceberg_filesystem_commit.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_filesystem>
    PARENT_SCOPE)
