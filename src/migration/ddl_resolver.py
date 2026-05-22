import os
from pathlib import Path
from typing import Dict, Any, Optional

import sqlglot
from pyspark.sql.types import StructField, StructType
from sqlglot import exp
import yaml

from paths import DATALAKE_SCRIPT_DIR, PROJECT_ROOT
from src.utils.source_rule_loader import load_all_layer_extra_fields


class DdlResolver:
    def __init__(self, source_rules: dict = None, extra_fields: dict = None):
        self.source_rules = source_rules
        # Global extra fields for models
        self.model_extra_fields = load_all_layer_extra_fields()

        if extra_fields is None:
            # src/core/ddl_parser.py -> src/core -> src -> project_root
            com_path = PROJECT_ROOT / "configs" / "rules" / "extra_fields" / "com.yaml"
            cur_path = PROJECT_ROOT / "configs" / "rules" / "extra_fields" / "cur.yaml"

            # 2. Load mapping configuration from YAML
            with open(com_path, 'r', encoding='utf-8') as f:
                com_extra_fields = yaml.safe_load(f)

            with open(cur_path, 'r', encoding='utf-8') as f:
                cur_extra_fields = yaml.safe_load(f)

            self.extra_fields = {
                "com": com_extra_fields,
                "cur": cur_extra_fields
            }

        config_path = PROJECT_ROOT / "configs" / "rules" / "data_type.yaml"

        self.config_path = config_path
        self.mapping = self._load_config()


    def _load_config(self):
        if not os.path.exists(self.config_path):
            return {"VARCHAR2": "StringType", "NUMBER": "DecimalType"}
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('hiveql_to_spark', {})


    def _file_has_create_table(self, path: Path) -> bool:
        try:
            content = path.read_text(encoding="utf-8").upper()
            return "CREATE TABLE" in content
        except Exception:
            return False

    def resolve_ddl_path(self, dml_path: Path) -> Optional[Path]:
        """
        DDL resolution priority:
        1. Same directory, same stem, same extension (DDL file contains 'CREATE TABLE' as first statement)
        2. Sibling directory named 'ddl/' with same stem
        3. Parent's sibling directory named 'ddl/' with same stem
        """
        candidates = [
            # dml_path.parent / dml_path.name,              # same file (multi-statement)
            # dml_path.parent / "ddl" / dml_path.name,      # sibling dir
            # dml_path.parent.parent / "ddl" / dml_path.name, # parent's sibling dir
            DATALAKE_SCRIPT_DIR / "ddl" / "com" / dml_path.name,  # parent's sibling dir
            DATALAKE_SCRIPT_DIR / "ddl" / "cur" / dml_path.name,  # parent's sibling dir
        ]

        # print(candidates)
        for c in candidates:
            if c.exists() and self._file_has_create_table(c):
                return c

        return None


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


    def enrich(self, pipeline_config: Dict[str, Any], model_type: str) -> Dict[str, Any]:
        # model_type = str(pipeline_config.get("model_type", "3")).lower()
        
        # Lấy extra fields từ model mapping, ghi đè nếu source_rules có quy định riêng
        model_layer: str = str(pipeline_config.get('layer'))
        extra_fields = self.model_extra_fields.get(model_layer).get(model_type, [])
        # source_extra_fields = self.source_rules.get("extra_ddl_fields", {}).get(model_type)
        # if source_extra_fields:
        #     extra_fields = source_extra_fields
        # print(model_layer, model_type)
        # print(self.model_extra_fields)

        # Lọc bỏ các cột được đánh dấu là non_original_field
        filtered_columns = [
            col for col in pipeline_config.get("columns", [])
            if col.get("remark") != "non_original_field"
        ]

        return {
            "columns": filtered_columns,
            "extra_fields": extra_fields,
            "model_type": model_type,
            "target_table_name": pipeline_config["target_table_name"],
            "pipeline_id": pipeline_config["pipeline_id"],
            "com_schema": "${com_schema}",
            "cur_schema": "${cur_schema}"
        }
