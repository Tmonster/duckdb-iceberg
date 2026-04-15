from scripts.data_generators.tests.base import IcebergTest
import os
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name, write_intermediates=False)

    def generate(self, catalog: str, *, target=None, connection_kwargs=None):
        if catalog != 'spark-rest':
            return

        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, LongType, UnknownType
        from pyiceberg.catalog import load_catalog

        # Start the Spark session via the normal connection so the catalog
        # infrastructure is warmed up, then create the table via PyIceberg.
        con = self.get_connection(catalog, target=target, **(connection_kwargs or {}))

        uri = os.getenv("ICEBERG_ENDPOINT", "http://127.0.0.1:8181")

        iceberg_catalog = load_catalog("demo", **{
            "type": "rest",
            "uri": uri,
        })

        identifier = f"default.{self.table}"
        if iceberg_catalog.table_exists(identifier):
            iceberg_catalog.drop_table(identifier)

        schema = Schema(
            NestedField(field_id=1, name="id",   field_type=LongType(),    required=True),
            NestedField(field_id=2, name="data",  field_type=UnknownType(), required=False),
        )

        table = iceberg_catalog.create_table(
            identifier,
            schema=schema,
            properties={"format-version": "3"},
        )

        print(f"Created table with schema: {table.schema()}")
        self.close_connection(con)
