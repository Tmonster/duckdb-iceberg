UPDATE iceberg_catalog.many_adds_deletes
SET schema_evol_added_col_1 = l_partkey_int
WHERE l_partkey_int % 5 = 0;