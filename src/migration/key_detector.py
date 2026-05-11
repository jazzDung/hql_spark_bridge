import logging
from typing import Optional
import sqlglot.expressions as exp
from src.migration.decomposer import DecomposedScript

class KeyDetectionError(Exception):
    pass

class KeyDetector:
    def __init__(self, ai_fallback: bool = False):
        self.ai_fallback = ai_fallback

    def detect(self, decomposed: DecomposedScript, columns: list, source_rules: dict) -> str:
        strategies = [
            ("source_rule_override", lambda: self._detect_from_source_rule(decomposed.base_table, source_rules)),
            ("ddl_heuristic",        lambda: self._detect_from_ddl(columns)),
            ("join_analysis",        lambda: self._detect_from_join_analysis(decomposed)),
            ("where_null_check",     lambda: self._detect_from_where_null_check(decomposed)),
            ("ai_fallback",          lambda: self._detect_via_ai(decomposed, columns) if self.ai_fallback else None),
        ]
        for strategy_name, strategy_fn in strategies:
            result = strategy_fn()
            if result:
                logging.info(f"Key detected via [{strategy_name}]: {result}")
                return result

        return "client_no"
        raise KeyDetectionError("All key detection strategies exhausted.")

    def _detect_from_source_rule(self, base_table: str, source_rules: dict) -> Optional[str]:
        # Simple glob-like matching implementation (e.g. "*_cif_alias")
        # For a full implementation, you would use `fnmatch`
        table_key_rules = source_rules.get("table_key_rules", [])
        for rule in table_key_rules:
            pattern = rule.get("pattern", "")
            if pattern.startswith("*") and base_table.endswith(pattern[1:]):
                return rule.get("key")
            elif pattern == base_table:
                return rule.get("key")
        return None

    def _detect_from_ddl(self, columns: list[dict]) -> Optional[str]:
        # Heuristic 1: column ending in 'id' that appears first and has no remark
        for col in columns:
            if col["name"].lower().endswith("id") and col.get("remark") is None:
                return col["name"]
        return None

    # def _detect_from_join_analysis(self, decomposed: DecomposedScript) -> Optional[str]:
    #     """
    #     Look for: ... LEFT JOIN {table}_bk o ON o.{col} = n.{col}
    #     """
    #     for block in decomposed.temp_tables:
    #         if block.name.endswith("_nw"):
    #             for node in block.ast_nodes:
    #                 joins = list(node.find_all(exp.Join))
    #                 for join in joins:
    #                     on_clause = join.find(exp.On)
    #                     if on_clause:
    #                         eq_nodes = list(on_clause.find_all(exp.EQ))
    #                         if eq_nodes:
    #                             col = eq_nodes[0].left.name
    #                             return col
    #     return None

    def _detect_from_join_analysis(self, decomposed: DecomposedScript) -> Optional[str]:
        """
        Look for: ... JOIN {table}_bk ON n.{col} = o.{col}
        """
        for block in decomposed.temp_tables:
            # Thông thường phép JOIN delta sẽ diễn ra ở bảng _nw hoặc _combined
            if block.name.endswith("_nw") or block.name.endswith("_combined"):
                for node in block.ast_nodes:
                    joins = list(node.find_all(exp.Join))
                    for join in joins:
                        # Lấy biểu thức của mệnh đề ON từ kwargs của node Join
                        on_expr = join.args.get("on")
                        if on_expr:
                            # Tìm phép toán bằng (=) bên trong mệnh đề ON
                            eq_nodes = list(on_expr.find_all(exp.EQ))

                            # Nếu bản thân mệnh đề ON đã là phép bằng (ví dụ: ON a.id = b.id)
                            if isinstance(on_expr, exp.EQ) and not eq_nodes:
                                eq_nodes = [on_expr]

                            if eq_nodes:
                                # eq_nodes[0].left thường là exp.Column (ví dụ: nw.cifid)
                                left_node = eq_nodes[0].left
                                if isinstance(left_node, exp.Column):
                                    return left_node.name  # Trả về tên cột (ví dụ: cifid)
        return None

    def _detect_from_where_null_check(self, decomposed: DecomposedScript) -> Optional[str]:
        for block in decomposed.temp_tables:
            if block.name.endswith("_nw"):
                for node in block.ast_nodes:
                    where_nodes = list(node.find_all(exp.Where))
                    for w in where_nodes:
                        is_null_nodes = list(w.find_all(exp.Is))
                        for isn in is_null_nodes:
                            if isinstance(isn.right, exp.Null):
                                col = isn.left
                                if hasattr(col, 'name'):
                                    return col.name
        return None

    def _detect_via_ai(self, decomposed: DecomposedScript, columns: list[dict]) -> Optional[str]:
        """
        Mocked AI fallback.
        """
        # Call Anthropic API here...
        return None
