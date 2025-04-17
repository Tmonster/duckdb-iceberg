insert into iceberg_catalog.many_adds_deletes
select * FROM iceberg_catalog.many_adds_deletes
where l_extendedprice_double < 30000