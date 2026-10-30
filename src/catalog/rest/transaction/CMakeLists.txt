add_library(iceberg_catalog_rest_transaction OBJECT
            iceberg_client_commit_verifier.cpp iceberg_rest_commit.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_rest_transaction>
    PARENT_SCOPE)
