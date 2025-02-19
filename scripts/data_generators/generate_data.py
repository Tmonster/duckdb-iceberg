from generate_spark_local.generate_iceberg_spark_local import IcebergSparkLocal
from generate_spark_rest.generate_iceberg_spark_rest import IcebergSparkRest

# Example usage:
if __name__ == "__main__":
    db2 = IcebergSparkRest()
    conn2 = db2.GetConnection()
    db2.GenerateTables(conn2)
    db2.CloseConnection(conn2)
    del db2
    del conn2
    db = IcebergSparkLocal()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn

