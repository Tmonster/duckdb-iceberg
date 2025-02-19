# README
Script used to generate test data for this repo. Don't use directly, but use makefile recipe `make data`

The script uses PySpark with the iceberg extension to generate datasets based on a starting
parquet file (tpch lineitem) and a series of updates/deletes/appends (see `./updates`). 
The script will output the table as a parquet file after every updates/deletes/appends along with the total count.
The base parquet file is also stored in the output dir.

# Valdation
- count(*) after each step
- full table copy to parquet file after each step

# Idea behind script:
- generated data easily within this repo
- contains all iceberg datatypes (currently WIP)
- contains nulls
- configurable scale factor
- verify behaviour matches spark

# Update queries
Should be portable between DuckDB, Spark and Snowflake

# Todo's:
- Arbitrary precision Decimals?
- Time not yet working
- PySpark does not support UUID
- Generate similar data from snowflake's iceberg implementation
- value deletes?


# How it works now

We have data generators, like spark-local, and spark-rest. Eventually we should have generators like DuckDB and more.

Each generate has a directory, in the directory are sub-directories with queries that create and modify a table, which should be defined in q00.
If some more setup is needed to generate the data, then you need to add it as a {setup.*} file in the directory with the other sql queries. 
A python script is the best bet for this.