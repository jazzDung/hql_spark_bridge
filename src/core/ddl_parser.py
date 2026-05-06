import sqlglot
from sqlglot import exp
import yaml
import os
from pyspark.sql.types import *
from pathlib import Path
from src.paths import *

class DDLParser:
    def __init__(self, config_path: Path = None):
        if config_path is None:
            # src/core/ddl_parser.py -> src/core -> src -> project_root
            project_root = PROJECT_ROOT
            config_path = project_root / "configs" / "rules" / "data_type.yaml"

        self.config_path = config_path
        self.mapping = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            return {"VARCHAR2": "StringType", "NUMBER": "DecimalType"}
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('hiveql_to_spark', {})

    def _map_to_spark_type(self, column_def):
        """
        Uses sqlglot AST to extract data types and parameters
        """
        data_type_exp = column_def.args['kind']
        # Get the base type name (e.g., DECIMAL, VARCHAR)
        base_type = data_type_exp.this.name.upper()

        # Get Spark Class name from YAML mapping
        spark_type_name = self.mapping.get(base_type, "StringType")

        try:
            if spark_type_name == "DecimalType":
                # Extract precision and scale from AST
                # Example: DECIMAL(18, 2) -> expressions[0]=18, expressions[1]=2
                params = data_type_exp.expressions
                p = int(params[0].this) if len(params) > 0 else 38
                s = int(params[1].this) if len(params) > 1 else 10
                return DecimalType(p, s)

            # For other types, use reflection to instantiate
            type_class = globals().get(spark_type_name)
            return type_class()
        except Exception:
            return StringType()

    def parse_hive_ddl(self, ddl_content: str):
        """
        Parses DDL using sqlglot with Hive dialect
        """
        try:
            # Parse SQL statement into AST
            # sqlglot automatically handles comments and non-standard formatting
            parsed_nodes = sqlglot.parse(ddl_content, read="hive")

            # Tìm node CREATE đầu tiên trong list
            create_node = next((node for node in parsed_nodes if isinstance(node, exp.Create)), None)

            if not create_node:
                raise ValueError("Không tìm thấy lệnh CREATE trong nội dung DDL")

            # 1. Extract table name (Database.Table)
            table_parts = create_node.this

            db_node = table_parts.args.get('db')
            db = db_node.this if db_node else "default"
            table = table_parts.this.this
            full_table_name = f"{db}.{table}"

            # 2. Iterate through column list in schema
            schema_fields = []

            # schema.this returns a list of ColumnDef objects
            schema_node = create_node.this.expressions
            if schema_node:
                for column_def in schema_node:
                    col_name = column_def.this.this  # Column name
                    spark_type = self._map_to_spark_type(column_def)

                    schema_fields.append(StructField(col_name, spark_type, True))

            return full_table_name, StructType(schema_fields)

        except Exception as e:
            print(f"ERROR: Failed to parse DDL with sqlglot: {e}")
            return "unknown.table", StructType([])
