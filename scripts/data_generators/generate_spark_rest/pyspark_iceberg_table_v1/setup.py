import duckdb
import os

PARQUET_SRC_FILE = os.getenv('PARQUET_SRC_FILE')

duckdb_con = duckdb.connect()
duckdb_con.execute("call dbgen(sf=0.001)")
duckdb_con.query("""CREATE VIEW test_table as
                    SELECT
                    (l_orderkey%2=0) as l_orderkey_bool,
                    l_partkey::INT32 as l_partkey_int,
                    l_suppkey::INT64 as l_suppkey_long,
                    l_extendedprice::FLOAT as l_extendedprice_float,
                    l_extendedprice::DOUBLE as l_extendedprice_double,
                    l_extendedprice::DECIMAL(9,2) as l_extendedprice_dec9_2,
                    l_extendedprice::DECIMAL(18,6) as l_extendedprice_dec18_6,
                    l_extendedprice::DECIMAL(38,10) as l_extendedprice_dec38_10,
                    l_shipdate::DATE as l_shipdate_date,
                    l_partkey as l_partkey_time,
                    l_commitdate::TIMESTAMP as l_commitdate_timestamp,
                    l_commitdate::TIMESTAMPTZ as l_commitdate_timestamp_tz,
                    l_comment as l_comment_string,
                    gen_random_uuid()::VARCHAR as uuid,
                    l_comment::BLOB as l_comment_blob
                    FROM
                    lineitem;""")

duckdb_con.execute(f"copy test_table to '{PARQUET_SRC_FILE}' (FORMAT PARQUET)")