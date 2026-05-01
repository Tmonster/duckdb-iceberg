from pyspark.sql import SparkSession
import os
from datetime import date
from pyspark.sql import Row

AWS_PROFILE  = os.environ["PROFILE"]
AWS_REGION   = os.environ["AWS_REGION"]
TABLE_BUCKET_ARN = os.environ["S3Table_arn"]
KMS_KEY_ARN = os.environ["AWS_KMS_KEY_ID"]

CATALOG_NAME     = "s3tablesbucket"

SPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.1"


_PACKAGES = ",".join([
    "software.amazon.awssdk:bundle:2.29.38",
    "com.github.ben-manes.caffeine:caffeine:3.1.8",
    "org.apache.commons:commons-configuration2:2.11.0",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.8",
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.13:{ICEBERG_VERSION}",
])


spark = (
    SparkSession.builder

    .appName("EncryptedIcebergS3Tables")
    # ── Iceberg / Spark SQL extensions ──────────────────────────────────────
    .config("spark.jars.packages", _PACKAGES)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── Register the S3 Tables catalog ──────────────────────────────────────
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl",
            "software.amazon.s3tables.iceberg.S3TablesCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", TABLE_BUCKET_ARN)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.client.region", AWS_REGION)
    # .config(f"spark.sql.catalog.{CATALOG_NAME}.http-client.proxy-endpoint", "http://localhost:8888")
    # .config("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService")
    # ── Iceberg-native encryption: use AWS KMS as the key management client ─
    # This enables Parquet Modular Encryption on every data file.
    # The master key ID is set per-table at CREATE TABLE time (Step 6).
    .config(f"spark.sql.catalog.{CATALOG_NAME}.encryption.kms-type", "aws")
    # ── Set as default catalog so table names don't need the prefix ─────────
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark session ready ✓")


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.demo.sales_simple_5 (
        sale_id     BIGINT,
        product     STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        -- This KMS key ID is used to wrap the per-file Parquet encryption keys.
        -- Every data file, delete file, manifest and manifest list will be
        -- encrypted end-to-end using Parquet Modular Encryption (AES-GCM).
        'encryption.key-id'               = '{KMS_KEY_ARN}'
    )
""")



print("Table created ✓")


rows = [
    Row(sale_id=1, product="Widget A"),
    Row(sale_id=2, product="Widget B"),
    Row(sale_id=3, product="Widget C"),
]

df = spark.createDataFrame(rows)

(
    df.writeTo(f"{CATALOG_NAME}.demo.sales_simple_5")
      .using("iceberg")
      .append()
)

print(f"Wrote {df.count()} rows ✓")

# Full table scan
spark.table(f"{CATALOG_NAME}.demo.sales_simple_5").show()

### Verify encryption

spark.sql(f"DESCRIBE EXTENDED {CATALOG_NAME}.demo.sales_simple").show(50, truncate=False)
