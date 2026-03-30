import sqlglot
from sqlglot import exp
import yaml
import os
from pyspark.sql.types import *


class DDLParser:
    def __init__(self, config_path=r"C:\Users\dungp\projects\hql_spark_bridge\configs\rules\data_type.yaml"):
        self.config_path = config_path
        self.mapping = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            return {"VARCHAR2": "StringType", "NUMBER": "DecimalType"}
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('data_type_mapping', {})

    def _map_to_spark_type(self, column_def):
        """
        Sử dụng AST của sqlglot để trích xuất kiểu dữ liệu và tham số
        """
        data_type_exp = column_def.args['kind']
        # Lấy tên kiểu cơ bản (ví dụ: DECIMAL, VARCHAR)
        base_type = data_type_exp.this.name.upper()

        # Lấy tên Spark Class từ YAML
        spark_type_name = self.mapping.get(base_type, "StringType")

        try:
            if spark_type_name == "DecimalType":
                # Trích xuất precision và scale từ AST
                # Ví dụ: DECIMAL(18, 2) -> expressions[0]=18, expressions[1]=2
                params = data_type_exp.expressions
                p = int(params[0].this) if len(params) > 0 else 38
                s = int(params[1].this) if len(params) > 1 else 10
                return DecimalType(p, s)

            # Với các kiểu khác, dùng reflection để khởi tạo
            type_class = globals().get(spark_type_name)
            return type_class()
        except Exception:
            return StringType()

    def parse_hive_ddl(self, ddl_content: str):
        """
        Parse DDL sử dụng sqlglot dialect Hive
        """
        try:
            # Parse câu lệnh SQL thành AST
            # sqlglot tự động xử lý comments và format lạ
            parsed = sqlglot.parse(ddl_content, read="hive")

            # 1. Lấy tên bảng (Database.Table)
            table_parts = parsed.this
            db = table_parts.args.get('db', exp.Identifier(this="default", quoted=False)).this
            table = table_parts.this.this
            full_table_name = f"{db}.{table}"

            # 2. Duyệt qua danh sách các cột trong schema
            schema_fields = []

            # schema.this trả về danh sách các ColumnDef
            for column_def in parsed.args['schema'].expressions:
                col_name = column_def.this.this  # Tên cột
                spark_type = self._map_to_spark_type(column_def)

                schema_fields.append(StructField(col_name, spark_type, True))

            return full_table_name, StructType(schema_fields)

        except Exception as e:
            print(f"ERROR: Failed to parse DDL with sqlglot: {e}")
            return "unknown.table", StructType([])

