# src/migration/decomposer.py

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import sqlglot
import sqlglot.expressions as exp
import os

from migration.utils import get_physical_dependencies
from src.utils.file_utils import parse_file_name


@dataclass
class TempTableBlock:
    name: str                    # ví dụ: "r_k2_cif_alias_bk"
    # suffix: str                  # ví dụ: "_bk"
    raw_sql: str                 # Đoạn text SQL được trích xuất
    ast_nodes: list              # Các node AST của sqlglot cho block này
    operation: str               # "create_as_select" | "drop_create_insert" | "insert_only"
    schema: str                  # schema của bản thân bảng tạm này
    dependencies: list[dict] = field(default_factory=list)

@dataclass
class DecomposedScript:
    source_name: str             # "k2"
    base_table: str              # "cif_alias"
    main_table: str              # "r_k2_cif_alias"
    pipeline_id: str             # "com_r_k2_cif_alias"
    schema: str                   # "com" | "raw" | "cur | "unl"
    sub_layer: str               # "r" | "t" | "m"
    temp_tables: list[TempTableBlock]
    main_sql: str                # Block INSERT INTO bảng đích (target table) cuối cùng
    raw_sql_full: str            # Nội dung gốc của file



class SqlDecomposer:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules

    def decompose(self, sql_file: Path) -> DecomposedScript:
        """
        Parse và chia nhỏ một file HiveQL nguyên khối thành các block có tên.
        """
        content = sql_file.read_text(encoding="utf-8")
        schema, sub_layer, source_name, base_table = parse_file_name(sql_file)
        main_table = f"{sub_layer}_{source_name}_{base_table}"

        # 1. Loại bỏ header comment block (các dòng bắt đầu bằng --)
        # content = self._strip_header(content)
        # 2. Loại bỏ các command đặc thù của Hive (SOURCE ..., SET ...)
        # content = self._clean_hive_specific(content)
        # 3. Parse AST
        statements = sqlglot.parse(content, read="hive", error_level=sqlglot.ErrorLevel.WARN)
        # 4. Nhóm các câu lệnh theo từng temp table
        blocks = self._group_by_table(statements)
        # 5. Nhận diện khối lệnh INSERT chính (bảng đích, không phải bảng tạm)
        main_sql, temp_blocks = self._separate_main(blocks, content, main_table)
        # 6. Build các object TempTableBlock
        temp_table_objs = self._build_temp_table_objects(temp_blocks, content)


        return DecomposedScript(
            source_name=source_name,
            base_table=base_table,
            main_table=main_table,
            pipeline_id=sql_file.stem,       # "com_r_k2_cif_alias"
            schema=schema,
            sub_layer=sub_layer,
            temp_tables=temp_table_objs,
            main_sql=main_sql,
            raw_sql_full=content
        )

    def _group_by_table(self, statements: list) -> dict[str, list]:
        """
        Trả về dict: { table_name: [stmt1, stmt2, ...] }
        """
        groups = {}
        for stmt in statements:
            # Trích xuất tên bảng từ bất kỳ loại câu lệnh nào
            table_name = self._extract_table_name(stmt)
            if table_name:
                groups.setdefault(table_name, []).append(stmt)
        return groups

    def _extract_table_name(self, stmt) -> Optional[str]:
        """
        Hoạt động với CREATE TABLE, DROP TABLE, INSERT INTO TABLE, TRUNCATE TABLE.
        Trả về tên bảng không bao gồm schema prefix.
        """
        if isinstance(stmt, (exp.Create, exp.Drop)):
            return stmt.find(exp.Table).name
        elif isinstance(stmt, exp.Insert):
            return stmt.find(exp.Table).name
        elif isinstance(stmt, exp.TruncateTable):
            return stmt.find(exp.Table).name
        return None

    def _extract_table_schema(self, stmt) -> Optional[str]:
        """
        Extract schema (db) of the table being created/inserted.
        """
        if isinstance(stmt, (exp.Create, exp.Drop)):
            return stmt.find(exp.Table).db
        elif isinstance(stmt, exp.Insert):
            return stmt.find(exp.Table).db
        elif isinstance(stmt, exp.TruncateTable):
            return stmt.find(exp.Table).db
        return None

    def _separate_main(self, blocks: dict, content: str, main_table_name: str) -> tuple[str, dict]:
        """
        Nhận diện bảng đích chính bằng phương pháp loại trừ:
        - Bất kỳ bảng nào có tên chứa hậu tố khớp với source_rules.temp_table_rules.*suffix là bảng tạm
        - Các bảng còn lại là ứng viên cho bảng đích
        - Bảng có khối lệnh INSERT INTO PARTITION cuối cùng = main
        """
        # skip_suffixes = self._get_skip_suffixes()   # ["_bk","_bf","_nw","_od"] từ source rules
        temp_blocks = {}
        main_stmts = []
        for table_name, stmts in blocks.items():
            if table_name != main_table_name:
                temp_blocks[table_name] = stmts
            else:
                main_stmts.extend(stmts)
        main_sql = "\n\n".join(f"{s.sql(dialect='hive', pretty=True)};" for s in main_stmts)
        return main_sql, temp_blocks


    def _build_temp_table_objects(self, temp_blocks: dict[str, list[exp.Expression]], original_content: str) -> list[TempTableBlock]:
        """
        Builds a list of TempTableBlock objects from the grouped temporary table statements.
        """
        temp_table_objs = []
        for table_name, statements in temp_blocks.items():
            raw_sql = "\n\n".join(f"{stmt.sql(dialect='hive', pretty=True)};" for stmt in statements)

            operation = "unknown"
            dependencies_dict = {}
            schema = "unknown"

            if statements:
                first_stmt = statements[0]

                # Get the schema of the temp table itself
                extracted_schema = self._extract_table_schema(first_stmt)
                if extracted_schema:
                    schema = extracted_schema

                if isinstance(first_stmt, exp.Create):
                    operation = "create_as_select"
                elif isinstance(first_stmt, exp.Insert):
                    operation = "insert_only"
                elif isinstance(first_stmt, exp.Drop):
                    # If a DROP is followed by a CREATE or INSERT, it's part of a drop_create_insert pattern
                    if len(statements) > 1 and (isinstance(statements[1], exp.Create) or isinstance(statements[1], exp.Insert)):
                        operation = "drop_create_insert"
                    else:
                        operation = "drop_only" # Or handle as needed, for now, just drop
                elif isinstance(first_stmt, exp.TruncateTable):
                    operation = "truncate_insert" # Assuming truncate is usually followed by insert

                # Extract dependencies (tables from FROM, JOIN etc. that are not the table being built)
                for stmt in statements:
                    dependencies_dict.update(get_physical_dependencies(stmt))

            dependencies = [{k: v} for k, v in dependencies_dict.items()]

            temp_table_objs.append(
                TempTableBlock(
                    name=table_name,
                    raw_sql=raw_sql,
                    ast_nodes=statements,
                    operation=operation,
                    schema=schema,
                    dependencies=dependencies
                )
            )
        return temp_table_objs


class DecomposerWriter:
    def write(self, decomposed: DecomposedScript, output_root: Path):
        steps_dir = output_root / "processing_steps"
        steps_dir.mkdir(parents=True, exist_ok=True)

        for block in decomposed.temp_tables:
            out_file = steps_dir / f"{block.name}.sql"
            out_file.write_text(block.raw_sql, encoding="utf-8")

        # Ghi riêng Main SQL để tham chiếu
        (steps_dir / "_main_dml.sql").write_text(decomposed.main_sql, encoding="utf-8")