import os
import yaml
from pathlib import Path
from sqlglot import exp

from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.transformers.basic_pyspark_transformer import BasicPySparkTransformer

class OptimizedPySparkTransformer(BaseSqlTransformer):
    """
    Objective 2: Optimize PySpark scripts.
    - Attempt to convert text file reading logic into JDBC reading if a rule exists.
    - Fallback to BasicPySparkTransformer logic if no rule is found.
    """
    
    def __init__(self, variable_mapping: dict, config_root: Path):
        self.variable_mapping = variable_mapping
        self.config_root = config_root
        # Create an instance of BasicTransformer to use as a fallback
        self.fallback_transformer = BasicPySparkTransformer(variable_mapping)

    def _extract_columns_from_create(self, node: exp.Create) -> list:
        """
        Extract the list of column names from a CREATE TABLE statement.
        """
        columns = []
        # schema contains column definitions: (col1 type1, col2 type2, ...)
        if node.args.get("this") and isinstance(node.args["this"], exp.Schema):
            for col_def in node.args["this"].expressions:
                if isinstance(col_def, exp.ColumnDef):
                    columns.append(col_def.name)
        return columns

    def _find_and_apply_optimization_rule(self, node: exp.Expression, context: SqlConversionContext):
        # Only apply to CREATE TABLE statements
        if not isinstance(node, exp.Create) or node.kind != "TABLE":
            return None

        # Find the corresponding rule file
        rule_path = self.config_root / "rules" / "optimizations" / f"{context.source_name}_{context.table_name}.yaml"
        
        if not rule_path.exists():
            return None # No rule found, will fallback

        with open(rule_path, 'r', encoding='utf-8') as f:
            rule = yaml.safe_load(f)

        if not rule.get("enabled", False):
            return None # Rule is disabled

        if rule.get("action", {}).get("type") == "generate_jdbc_read":
            params = rule["action"]["parameters"]
            
            # --- NEW LOGIC: AUTOMATICALLY EXTRACT COLUMNS FROM CREATE TABLE ---
            columns = self._extract_columns_from_create(node)
            
            # If query_template exists in config, populate it with column names
            if "query_template" in params and columns:
                # Format columns into a string: col1,\n    col2,\n    ...
                cols_str = ",\n    ".join(columns)
                query = params["query_template"].format(
                    columns=cols_str, 
                    source_db_table=params["source_db_table"]
                )
            else:
                # Fallback to hardcoded query if available (backward compatibility)
                query = params.get("query", "")

            # Trả về một dictionary chứa thông tin cần thiết để sinh code
            return {
                "type": "jdbc_read",
                "jdbc_url_variable": params["jdbc_url_variable"],
                "query": query,
                "source_db_table": params["source_db_table"]
            }
            
        return None

    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        optimized_blocks = []

        # --- Xử lý Header Comments ---
        python_header_comments = []
        if context.header_comments:
            lines = context.header_comments.split('\n')
            # Lọc bỏ các dòng trống ở đầu/cuối khối comment nếu có
            lines = [line for line in lines if line.strip()]

            if len(lines) > 1:
                # Nếu là comment nhiều dòng, dùng docstring của Python
                python_header_comments.append('"""')
                for line in lines:
                    # Loại bỏ '--' và khoảng trắng thừa, sau đó thêm vào docstring
                    if line.strip().startswith('--'):
                        python_header_comments.append(line.strip()[2:].strip())
                    else:
                        python_header_comments.append(line.strip())
                python_header_comments.append('"""')
            elif len(lines) == 1:
                # Nếu là comment một dòng, dùng '#' của Python
                line = lines[0]
                if line.strip().startswith('--'):
                    python_header_comments.append(f"# {line.strip()[2:].strip()}")
                else:
                    python_header_comments.append(f"# {line.strip()}")

        formatted_header_comments = '\n'.join(python_header_comments)
        # --- Kết thúc xử lý Header Comments ---

        
        for node in context.ast_nodes:
            optimization_result = self._find_and_apply_optimization_rule(node, context)
            
            if optimization_result:
                # If an optimization rule is found, add the JDBC block to the list
                optimized_blocks.append(optimization_result)
                # Skip the original CREATE TABLE node, do not translate it
            else:
                # If no rule is found, use the fallback transformer to translate this node
                # Create a temporary context containing only the current node
                temp_context = SqlConversionContext(
                    original_file_path=context.original_file_path,
                    raw_sql_content=node.sql(),
                    source_name=context.source_name,
                    table_name=context.table_name,
                    ast_nodes=[node]
                )
                fallback_model = self.fallback_transformer.transform(temp_context)
                # Add the translated queries to the list
                optimized_blocks.extend([
                    {"type": "raw_sql", "query": q} for q in fallback_model.transformed_queries
                ])

        render_model = JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            header_comments=formatted_header_comments,
        )
        # Add dynamic attribute
        render_model.optimized_blocks = optimized_blocks
        
        return render_model
