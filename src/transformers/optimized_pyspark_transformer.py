import yaml
from pathlib import Path
from sqlglot import exp

from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.transformers.basic_pyspark_transformer import BasicPySparkTransformer
from src.transformers.utils import format_header_comments, _handle_skip_action, \
    handle_generate_jdbc_read_action, is_rule_triggered


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
            "skip": _handle_skip_action,
            "generate_jdbc_read": handle_generate_jdbc_read_action,
        }

        for name, rule in config["rules"].items():
            if not rule.get("enabled", False):
                continue

            if is_rule_triggered(rule, node, context):
                action_type = rule.get("action", {}).get("type")
                handler = action_handlers.get(action_type)
                
                if handler:
                    # A rule was triggered and a handler exists, stop processing more rules.
                    return handler(rule, node, context)
                
                # Stop at the first triggered rule, even if the action is unknown.
                return None 

        return None # No matching rule found

    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        optimized_blocks = []

        formatted_header_comments = format_header_comments(context.header_comments)

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
