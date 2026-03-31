import os
import yaml
from pathlib import Path
from sqlglot import exp

from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.transformers.basic_pyspark_transformer import BasicPySparkTransformer
from src.core.ext_parser import ExtParser

class OptimizedPySparkTransformer(BaseSqlTransformer):
    """
    Objective 2: Optimize PySpark scripts.
    - Attempt to convert text file reading logic into JDBC reading if a rule exists.
    - Fallback to BasicPySparkTransformer logic if no rule is found.
    """
    
    def __init__(self, variable_mapping: dict, config_root: Path):
        self.variable_mapping = variable_mapping
        self.config_root = config_root
        self.fallback_transformer = BasicPySparkTransformer(variable_mapping)

    def _extract_columns_from_create(self, node: exp.Create) -> list:
        """Extract the list of column names from a CREATE TABLE statement."""
        columns = []
        if node.args.get("this") and isinstance(node.args["this"], exp.Schema):
            for col_def in node.args["this"].expressions:
                if isinstance(col_def, exp.ColumnDef):
                    columns.append(col_def.name)
        return columns

    def _is_rule_triggered(self, rule: dict, node: exp.Expression, context: SqlConversionContext) -> bool:
        """Check if the trigger conditions for a rule are met."""
        trigger = rule.get("trigger", {})
        condition_match_result = []

        def map_trigger_to_node_type(node_type:str):
            node_type = node_type.lower()
            
            if node_type == "create":
                return exp.Create
            elif node_type == "drop":
                return exp.Drop
            elif node_type == "alter":
                return exp.Alter
            elif node_type == "insert":
                return exp.Insert
            else:
                # It's crucial to provide clear feedback when an unexpected value is encountered.
                # This prevents silent failures and helps in debugging.
                raise ValueError(
                    f"Unrecognized SQL command string: '{node_type}'. "
                    "Expected 'Create' or 'Drop'."
                )
        
        for condition in trigger:
            if condition == "node_type":
                try:
                    node_type = map_trigger_to_node_type(trigger["node_type"])
                    if isinstance(node, node_type):
                        condition_match_result.append(True)
                    else:
                        condition_match_result.append(False)
                except:
                    condition_match_result.append(False)

            if condition == "table_name_suffix":
                try:
                    suffixes = tuple(trigger["table_name_suffix"])
                    # Get the table name from the AST node
                    if isinstance(node, exp.Create) :
                        table_name = node.this.this.this.this
                    elif isinstance(node, tuple([exp.Drop, exp.Alter, exp.Insert])):
                        table_name = node.this.this.this
                    else:
                        raise ValueError(f"Unsupported node type: {node_type}")

                    print(f"table_name: {table_name}, suffix: {suffixes}, result: {table_name.endswith(suffixes)}")

                    if table_name.endswith(suffixes):
                        condition_match_result.append(True)
                    else:
                        condition_match_result.append(False)
                except:
                    condition_match_result.append(False)

        print(f"condition_match_result for {type(node)}: {condition_match_result}, {all(condition_match_result)}")
        
        return all(condition_match_result)

    def _handle_skip_action(self, rule: dict, node: exp.Expression, context: SqlConversionContext) -> dict:
        """Handle the 'skip' action."""
        # Add logging here in the future if needed for traceability

        return {"type": "skip"}

    def _handle_generate_jdbc_read_action(self, rule: dict, node: exp.Expression, context: SqlConversionContext) -> dict:
        """Handle the 'generate_jdbc_read' action with robustness checks."""
        action = rule.get("action", {})
        params = {**rule, **action.get("parameters", {})}
        columns = self._extract_columns_from_create(node)
        
        query = ""
        if "query_template" in params and columns:
            cols_str = ",\n    ".join(columns)
            query = params["query_template"].format(
                columns=cols_str,
                source_db_table=f"{params.get('schema', '')}.{context.table_name}"
            )
        else:
            ext_file_path = self.config_root.parent /"samples" / "input" / "ext" / "xml" / f"{context.source_name}_{context.table_name}.xml"
            parsed_ext = ExtParser.parse_ext(ext_file_path)
            query = parsed_ext.query
            # query = params.get("query", "")

        return {
            "type": "jdbc_read",
            "jdbc_url_variable": params.get("jdbc_url_variable"),
            "query": query,
            "source_db_table": f"{params.get('schema', '')}.{context.table_name}"
        }

    def _find_and_apply_optimization_rule(self, node: exp.Expression, context: SqlConversionContext):
        """
        Finds and applies the first matching optimization rule for a given AST node.
        This function now acts as a dispatcher, delegating the action handling to specific methods.
        """
        # if not isinstance(node, exp.Create) or node.kind != "TABLE":
        #     return None

        rule_file_path = self.config_root / "rules" / "optimizations" / f"{context.source_name}.yaml"
        if not rule_file_path.exists():
            return None

        with open(rule_file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if not config or not config.get("rules"):
            return None

        action_handlers = {
            "skip": self._handle_skip_action,
            "generate_jdbc_read": self._handle_generate_jdbc_read_action,
        }

        for name, rule in config["rules"].items():
            if not rule.get("enabled", False):
                continue

            if self._is_rule_triggered(rule, node, context):
                action_type = rule.get("action", {}).get("type")
                handler = action_handlers.get(action_type)
                
                if handler:
                    # A rule was triggered and a handler exists, stop processing more rules.
                    return handler(rule, node, context)
                
                # Stop at the first triggered rule, even if the action is unknown.
                return None 

        return None # No matching rule found

    def _format_header_comments(self, header_comments: str) -> str:
        """Converts SQL header comments into Python-style comments or docstrings."""
        python_header_comments = []
        if header_comments:
            lines = header_comments.split('\n')
            lines = [line for line in lines if line.strip()]
            if len(lines) > 1:
                python_header_comments.append('"""')
                for line in lines:
                    clean_line = line.strip().lstrip('-').strip()
                    python_header_comments.append(clean_line)
                python_header_comments.append('"""')
            elif len(lines) == 1:
                clean_line = lines[0].strip().lstrip('-').strip()
                python_header_comments.append(f"# {clean_line}")
        return '\n'.join(python_header_comments)

    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        optimized_blocks = []

        formatted_header_comments = self._format_header_comments(context.header_comments)

        for node in context.ast_nodes:
            print(f"Optimize for {type(node)}")
            optimization_result = self._find_and_apply_optimization_rule(node, context)
            
            if optimization_result:
                if optimization_result.get("type") == "skip":
                    continue
                optimized_blocks.append(optimization_result)
            else:
                temp_context = SqlConversionContext(
                    original_file_path=context.original_file_path,
                    raw_sql_content=node.sql(),
                    source_name=context.source_name,
                    table_name=context.table_name,
                    ast_nodes=[node]
                )
                fallback_model = self.fallback_transformer.transform(temp_context)
                optimized_blocks.extend([
                    {"type": "raw_sql", "query": q} for q in fallback_model.transformed_queries
                ])

        render_model = JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            header_comments=formatted_header_comments,
        )
        render_model.optimized_blocks = optimized_blocks
        return render_model
