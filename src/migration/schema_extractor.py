import re
from pathlib import Path
from sqlglot import parse
from sqlglot import exp


class SchemaExtractor:
    def __init__(self, source_rules: dict):
        self.non_original_fields = set(
            source_rules.get("column_rules", {})
            .get("non_original_fields", {})
            .get("name", [])
        )

    def extract(self, ddl_path: Path) -> list[dict]:
        ddl_content = ddl_path.read_text(encoding="utf-8")

        # Remove custom SOURCE statements if exists
        ddl_content = re.sub(
            r"source\s+\S+;",
            "",
            ddl_content,
            flags=re.IGNORECASE
        )

        statements = parse(ddl_content, read="hive")

        create_stmt = next(
            (
                stmt for stmt in statements
                if isinstance(stmt, exp.Create)
            ),
            None
        )

        if create_stmt is None:
            raise ValueError("No CREATE TABLE statement found")

        schema = create_stmt.this

        if not isinstance(schema, exp.Schema):
            raise ValueError("Unable to parse schema from DDL")

        result = []

        for column in schema.expressions:
            if not isinstance(column, exp.ColumnDef):
                continue

            col_name = column.name.lower()

            kind = column.args.get("kind")
            hive_type = kind.sql(dialect="hive").upper() if kind else "STRING"

            remark = (
                "non_original_field"
                if col_name in self.non_original_fields
                else None
            )

            result.append({
                "name": col_name,
                "type": hive_type,
                "remark": remark
            })

        return result