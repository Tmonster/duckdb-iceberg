import os
from pyspark.sql import SparkSession

client_id = os.getenv('POLARIS_CLIENT_ID', '')
client_secret = os.getenv('POLARIS_CLIENT_SECRET', '')

if client_id == '' or client_secret == '':
    print("no client_id or client_secret")
    exit(1)

spark = (SparkSession.builder
         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
         .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.23.19,software.amazon.awssdk:url-connection-client:2.23.19")
         .config('spark.sql.iceberg.vectorization.enabled', 'false')
         # Configure the 'polaris' catalog as an Iceberg rest catalog
         .config("spark.sql.catalog.quickstart_catalog.type", "rest")
         .config("spark.sql.catalog.quickstart_catalog", "org.apache.iceberg.spark.SparkCatalog")
         # Specify the rest catalog endpoint
         .config("spark.sql.catalog.quickstart_catalog.uri", "http://localhost:8181/api/catalog")
         # Enable token refresh
         .config("spark.sql.catalog.quickstart_catalog.token-refresh-enabled", "true")
         # specify the client_id:client_secret pair
         .config("spark.sql.catalog.quickstart_catalog.credential", f"{client_id}:{client_secret}")
         # Set the warehouse to the name of the catalog we created
         .config("spark.sql.catalog.quickstart_catalog.warehouse", "quickstart_catalog")
         # Scope set to PRINCIPAL_ROLE:ALL
         .config("spark.sql.catalog.quickstart_catalog.scope", 'PRINCIPAL_ROLE:ALL')
         # Enable access credential delegation
         .config("spark.sql.catalog.quickstart_catalog.header.X-Iceberg-Access-Delegation", 'vended-credentials')
         .config("spark.sql.catalog.quickstart_catalog.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
         .config("spark.sql.catalog.quickstart_catalog.s3.region", "us-west-2")
         .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")).getOrCreate()


spark.sql("USE quickstart_catalog")
spark.sql("SHOW NAMESPACES").show()

spark.sql("CREATE NAMESPACE IF NOT EXISTS COLLADO_TEST")
spark.sql("USE NAMESPACE COLLADO_TEST")
spark.sql("""
CREATE TABLE IF NOT EXISTS quickstart_table (
  id BIGINT, data STRING
)
USING ICEBERG
""")
spark.sql("INSERT INTO quickstart_table VALUES (1, 'some data'), (2, 'more data'), (3, 'yet more data')")
spark.sql("SELECT * FROM quickstart_table").show()

