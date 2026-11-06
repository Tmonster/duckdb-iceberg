add_library(
  iceberg_catalog_glue_hive OBJECT
  glue_info.cpp
  glue_types.cpp
  hive_multi_file_reader.cpp
  hive_stats.cpp
  iceberg_glue_hive_table.cpp
  iceberg_glue_hive_api.cpp
  iceberg_glue_hive_insert.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:iceberg_catalog_glue_hive>
    PARENT_SCOPE)
