from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
import random
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import Row
from datetime import datetime, timedelta
from pyspark import SparkContext
import sys
import os


CONNECTION_KEY = 'spark-rest'
SPARK_RUNTIME_PATH = 'scripts/data_generators/iceberg-spark-runtime-3.5_2.12-1.9.0.jar'
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0 pyspark-shell"
)
# os.environ["AWS_REGION"] = "us-east-1"
# os.environ["AWS_ACCESS_KEY_ID"] = "admin"
# os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
# spark = (
#     SparkSession.builder.appName("DuckDB REST Integration test")
#     .config(
#         "spark.sql.extensions",
#         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     )
#     .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.demo.type", "rest")
#     .config("spark.sql.catalog.demo.uri", "http://127.0.0.1:8181")
#     .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/")
#     .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
#     .config("spark.sql.catalog.demo.s3.path-style-access", "true")
#     .config('spark.driver.memory', '10g')
#     .config("spark.sql.catalogImplementation", "in-memory")
#     .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
#     .config('spark.jars', SPARK_RUNTIME_PATH)
#     .getOrCreate()
# )

spark = (
    SparkSession.builder.appName("DuckDB REST Integration test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config('spark.sql.defaultCatalog', 'spark_catalog')
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "rest")
    .config("spark.sql.catalog.spark_catalog.uri", "https://s3tables.us-east-2.amazonaws.com/iceberg")
    .config("spark.sql.catalog.spark_catalog.warehouse", "arn:aws:s3tables:us-east-2:840140254803:bucket/new-iceberg-bucket")
    # .config("spark.sql.catalog.spark_catalog.metrics-reporter-impl", "null")
    .config("spark.sql.catalog.spark_catalog.s3.path-style-access", "true")
    .config('spark.sql.catalog.spark_catalog.rest.auth.type','sigv4')
    .config('spark.sql.catalog.spark_catalog.rest.signing-name','s3tables')
    .config('spark.sql.catalog.spark_catalog.rest.signing-region', 'us-east-2')
    .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config('spark.driver.memory', '10g')
    .config("spark.sql.catalogImplementation", "in-memory")
    .config('spark.jars', SPARK_RUNTIME_PATH)
    .getOrCreate()
)

table_identifier = "default.large_partitioned_table_for_vacuum"

try:
    spark.sql(f"""
delete from {table_identifier} where 1=1;
""")
# if table not found, just continue
except:
    spark.sql(f"""
        CREATE TABLE {table_identifier} (id int, name string, joined timestamp)
        USING iceberg
        PARTITIONED BY (month(joined))
        TBLPROPERTIES (
        'format-version'=2, 
        'write.update.mode'='merge-on-read', 
        'write.delete.mode'='merge-on-read',
        'write.metadata.metrics.default' = 'none')
    """)



start_time = datetime(2000, 1, 1)
end_time = datetime(2024, 12, 31)

current_time = start_time
id_counter = 1

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("joined", TimestampType(), False)
])

for snapshot_num in range(1, 5):
    # Random interval between 10 and 10 days
    delta_days = random.randint(1, 4)
    current_time += timedelta(days=delta_days)
    if current_time > end_time:
        print("end time reached, exiting")
        break

    # Generate 5–10 random rows per snapshot
    num_rows = random.randint(5, 10)
    rows = []
    for _ in range(num_rows):
        rows.append(Row(
            id=id_counter,
            name=f"User_{id_counter}",
            joined=current_time
        ))
        id_counter += 1

    # Create DataFrame
    df = spark.createDataFrame(rows, schema=schema)

    # Append to table (creates a new snapshot)
    df.writeTo(table_identifier).append()

    print(f"Inserted snapshot {snapshot_num} at time {current_time}")