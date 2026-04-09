import json
import os
import sys

from src.core.type_resolver import TypeResolver
from src.discovery.base_inspector import BaseInspector


class MSSQLInspector(BaseInspector):
    def __init__(self, conn_config, spark_session=None):
        super().__init__(spark_session, conn_config)
        self.engine_type = self._determine_engine()

    def _determine_engine(self):
        """Check whether to use Spark or Native Python"""
        if self.spark is not None:
            try:
                # Test if Spark can load the MSSQL Driver
                self.spark.range(1).limit(0).collect()
                print("INFO: Using Spark Engine for discovery.")
                return "SPARK"
            except Exception as e:
                print(f"WARN: Spark found but driver issue: {e}. Falling back to Native.")

        print("INFO: Using Native Python Engine (pymssql) for discovery.")
        return "NATIVE"

    def get_schema(self, table_name):
        if self.engine_type == "SPARK":
            return self._get_schema_via_spark(table_name)
        else:
            return self._get_schema_via_native(table_name)

    def _get_schema_via_spark(self, table_name):
        """Old logic using Spark JDBC"""
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config['url']) \
            .option("dbtable", table_name) \
            .option("user", self.config['user']) \
            .option("password", self.config['password']) \
            .load().limit(0)

        return [{"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
                for f in df.schema.fields]

    def _get_schema_via_native(self, table_name):
        """New logic using pymssql to read metadata directly from SQL Server"""
        import pymssql  # pip install pymssql

        # Extract host and port from JDBC URL or config
        # Assuming config has host, user, password, database
        conn = pymssql.connect(
            server=self.config['host'],
            user=self.config['user'],
            password=self.config['password'],
            database=self.config['database']
        )

        cursor = conn.cursor(as_dict=True)
        # Query INFORMATION_SCHEMA to get standard metadata
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name.split('.')[-1]}'
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        # Manually map SQL Type to Spark Type Name to sync with Spark Engine
        # (Reusing logic from your data_type.yaml)
        schema_json = []
        for row in rows:
            schema_json.append({
                "name": row['COLUMN_NAME'],
                "type": TypeResolver.convert_datatype(row['DATA_TYPE'], input_dialect='mssql', output_dialect='canonical_types'),
                # "type": self._map_native_type_to_spark(row['DATA_TYPE']),
                "nullable": True if row['IS_NULLABLE'] == 'YES' else False
            })
        return schema_json

    def _map_native_type_to_spark(self, native_type):
        """Ensure Native output matches Spark output"""
        mapping = {
            "varchar": "StringType",
            "nvarchar": "StringType",
            "int": "IntegerType",
            "bigint": "LongType",
            "datetime": "TimestampType",
            "decimal": "DecimalType(38,10)"
        }
        return mapping.get(native_type.lower(), "StringType")

    def get_sample_query(self, table_name):
        # MSSQL specific, use WITH (NOLOCK) to avoid locking production tables
        return f"SELECT * FROM {table_name} WITH (NOLOCK)"
