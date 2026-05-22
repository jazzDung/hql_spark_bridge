import logging
from typing import Optional
from collections import Counter
import sqlglot
import sqlglot.expressions as exp
from migration.com.decomposer import ComDecomposedScript
from src.migration.ddl_resolver import DdlResolver
from src.migration.schema_extractor import SchemaExtractor

class ComKeyDetector:
    def __init__(self):
        pass

    def detect(self, decomposed: ComDecomposedScript, source_rules: dict) -> dict:
        """
        Phân tích script SQL và suy diễn Logical Primary Key dựa trên các heuristic.
        Đầu ra là một dictionary bao gồm:
        - logical_primary_key: list of strings (column names)
        - confidence: 'HIGH' | 'MEDIUM' | 'LOW'
        - reasoning: Giải thích pattern nào đã được match
        
        Áp dụng cơ chế fallback: ưu tiên các pattern có độ tin cậy cao nhất.
        """

        strategies = [
            ("golden_clue_window", self._detect_from_window_function),
            ("ddl_explicit", self._detect_from_ddl),
            ("silver_clue_join", self._detect_from_join),
            ("source_rule_override", self._detect_from_source_rule),
        ]

        for strategy_name, strategy_fn in strategies:
            result = strategy_fn(decomposed, source_rules)
            if result and result.get("logical_primary_key"):
                logging.info(f"Key detected via [{strategy_name}]: {result['logical_primary_key']}")
                return result

        # Fallback nếu không pattern nào match
        return {
            "logical_primary_key": [],
            "confidence": "LOW",
            "reasoning": "No patterns matched. Fallback to empty."
        }

    def _get_all_statements(self, decomposed: ComDecomposedScript) -> list:
        try:
            main_stmts = sqlglot.parse(decomposed.main_sql, read="hive", error_level=sqlglot.ErrorLevel.IGNORE)
        except Exception:
            main_stmts = []
        
        stmts = [s for s in main_stmts if s is not None]
        for block in reversed(decomposed.temp_tables):
            stmts.extend(block.ast_nodes)
        return stmts

    def _get_schema(self, decomposed: ComDecomposedScript, source_rules: dict) -> list[str]:
        ddl_resolver = DdlResolver(source_rules=source_rules)
        ddl_path = ddl_resolver.resolve_ddl_path(decomposed.file_path)

        schema_extractor = SchemaExtractor(source_rules=source_rules)
        columns = schema_extractor.extract(ddl_path)

        return [column['name'] for column in columns]

    def _extract_window_keys(self, window: exp.Window) -> list[str] | None:
        # Phải là hàm ROW_NUMBER
        if isinstance(window.this, exp.RowNumber):
            partition_by = window.args.get("partition_by")
            if partition_by:
                keys = []
                for p_expr in partition_by:
                    # Bẫy hàm lồng nhau: dùng find_all(exp.Column) để bóc trần qua các hàm (trim, coalesce...)
                    for col in p_expr.find_all(exp.Column):
                        if col.name and col.name not in keys:
                            keys.append(col.name)
            else:
                return None

        return keys

    def _detect_from_window_function(self, decomposed: ComDecomposedScript, source_rules: dict) -> Optional[dict]:
        """
        Quét toàn bộ AST, tìm mọi hàm ROW_NUMBER, trích xuất các tập hợp key
        và thống kê tần suất xuất hiện của chúng.
        """
        # Sử dụng Counter để đếm tần suất các nhóm key
        key_groups_counter = Counter()
        stmts = self._get_all_statements(decomposed)
        schema = self._get_schema(decomposed, source_rules)

        for ast in stmts:
            # Quét toàn bộ cây AST để tìm TẤT CẢ các node Window
            for window in ast.find_all(exp.Window):
                keys = self._extract_window_keys(window)
                if not keys:
                    continue

                if any(column.lower() not in schema for column in keys):
                    continue

                else:
                    # List không thể hash trong Python, nên ta ép sang Tuple để làm key cho Counter
                    key_tuple = tuple(keys)
                    key_groups_counter[key_tuple] += 1

        # Format lại kết quả đầu ra, sắp xếp theo tần suất giảm dần (most_common)
        if key_groups_counter:
            key_tuple, count = key_groups_counter.most_common(1)[0]
            return {
                "logical_primary_key": list(key_tuple),
                "frequency": count,
                # Tự động nâng/hạ confidence dựa trên tần suất lặp lại
                "confidence": "HIGH" if count > 1 else "MEDIUM",
                "reasoning": f"Found ROW_NUMBER() PARTITION BY matching this exact group {count} times."
            }
        else:
            return None

    def _detect_from_ddl(self, decomposed: ComDecomposedScript, source_rules: dict) -> Optional[dict]:
        """
        Heuristic 3: DDL Explicit (Khai báo tường minh) - Độ tin cậy: HIGH
        Tìm node exp.PrimaryKeyColumnConstraint bên trong exp.ColumnDef của câu lệnh CREATE.
        """
        for block in decomposed.temp_tables:
            for stmt in block.ast_nodes:
                if isinstance(stmt, exp.Create):
                    schema = stmt.find(exp.Schema)
                    if schema:
                        for col_def in schema.find_all(exp.ColumnDef):
                            for constraint in col_def.find_all(exp.PrimaryKeyColumnConstraint):
                                if col_def.name:
                                    return {
                                        "logical_primary_key": [col_def.name],
                                        "confidence": "HIGH",
                                        "reasoning": "Explicit PrimaryKeyColumnConstraint found in DDL."
                                    }
        return None

    def _detect_from_join(self, decomposed: ComDecomposedScript, source_rules: dict) -> Optional[dict]:
        """
        Heuristic 2: The Silver Clue (Trục Join) - Độ tin cậy: MEDIUM
        Tìm các cột được dùng làm điều kiện map giữa các bảng trong điều kiện ON.
        Chỉ lấy nếu cột này được lặp lại ở >= 2 lệnh LEFT JOIN.
        """
        stmts = self._get_all_statements(decomposed)
        join_columns = {}
        
        for stmt in stmts:
            for join in stmt.find_all(exp.Join):
                # Chỉ lấy mệnh đề LEFT JOIN
                side = join.args.get("side")
                if side and side.upper() == "LEFT":
                    on_expr = join.args.get("on")
                    if on_expr:
                        eq_nodes = list(on_expr.find_all(exp.EQ))
                        
                        # Nếu bản thân mệnh đề on_expr đã là 1 phép EQ
                        if isinstance(on_expr, exp.EQ) and not eq_nodes:
                            eq_nodes = [on_expr]
                        
                        cols_in_join = set()
                        for eq in eq_nodes:
                            # Lấy tên cột từ cả vế trái và phải (tương ứng với table.column)
                            if isinstance(eq.left, exp.Column):
                                cols_in_join.add(eq.left.name)
                            if isinstance(eq.right, exp.Column):
                                cols_in_join.add(eq.right.name)
                        
                        for col in cols_in_join:
                            join_columns[col] = join_columns.get(col, 0) + 1

        # Lọc ra những column lặp lại từ 2 lệnh LEFT JOIN trở lên
        potential_keys = [col for col, count in join_columns.items() if count >= 2]
        
        if potential_keys:
            return {
                "logical_primary_key": potential_keys,
                "confidence": "MEDIUM",
                "reasoning": "Columns repeatedly used in >= 2 LEFT JOIN conditions."
            }
        
        return None

    def _detect_from_source_rule(self, decomposed: ComDecomposedScript, source_rules: dict) -> Optional[dict]:

        # Simple glob-like matching implementation (e.g. "*_cif_alias")
        # For a full implementation, you would use `fnmatch`

        base_table = decomposed.base_table
        table_key_rules = source_rules.get("table_key_rules", [])
        for rule in table_key_rules:
            pattern = rule.get("pattern", "")
            if pattern.startswith("*") and base_table.endswith(pattern[1:]):
                return {
                    "logical_primary_key": [rule.get("key")],
                    "confidence": "LOW",
                    "reasoning": "Can not find using pattern. Fallback to source's default"
                }
            elif pattern == base_table:
                return {
                    "logical_primary_key": [rule.get("key")],
                    "confidence": "LOW",
                    "reasoning": "Can not find using pattern. Fallback to source's default"
                }

        return None

    def _detect_via_ai(self, decomposed: ComDecomposedScript, columns: list[dict]) -> Optional[str]:
        """
        Mocked AI fallback.
        """
        # Call Anthropic API here...
        return None