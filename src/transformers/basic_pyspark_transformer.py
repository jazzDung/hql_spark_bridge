import yaml
from sqlglot import exp
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.paths import VARIABLE_CONFIG_PATH
from src.transformers.utils import (
    is_comment_only,
    format_header_comments,
    replace_variables_in_node,
    replace_variables_in_strings, remove_non_standard_fields, rename_remaining_identifiers_and_tables_for_cur
)

class BasicPySparkTransformer(BaseSqlTransformer):
    """
    Goal 1: Wrap the original SQL logic into PySpark (for easier BA readability).
    - Transform Hive configuration variables (${raw_schema}) into Python variables ({params["raw_schema"]}).
    - Convert dialect from hive -> spark.
    """
    
    def __init__(self, variable_mapping: dict = None):
        """
        Receives mapping rules from the variable.yaml file
        Example: {'raw_schema': 'params["raw_schema"]', 'batch_date': 'batch_date'}
        """

        if variable_mapping is None:
            # 2. Load mapping configuration from YAML
            with open(VARIABLE_CONFIG_PATH, 'r', encoding='utf-8') as f:
                self.variable_mapping = yaml.safe_load(f)

        else:
            self.variable_mapping = variable_mapping

    def transform(self, context: SqlConversionContext, dialect: str = 'pyspark') -> JinjaRenderModel:
        transformed_queries = []
        is_partitioned = False

        # --- Process Header Comments ---
        formatted_header_comments = format_header_comments(context.header_comments)

        for node in context.ast_nodes:
            # 1. Scan the AST tree to replace variables (Parameter/Var)
            replace_variables_in_node(node, self.variable_mapping, dialect)

            # 2. Process variables embedded in STRINGS (with single quotes), called Literal by sqlglot
            replace_variables_in_strings(node, self.variable_mapping, dialect)

            node = remove_non_standard_fields(node)
            node = rename_remaining_identifiers_and_tables_for_cur(node)

            # 3. Check if the table is partitioned (for Jinja to know whether to call drop_partition command)
            if isinstance(node, exp.Create) and node.args.get("properties"):
                for prop in node.args["properties"].expressions:
                    if isinstance(prop, exp.PartitionedByProperty):
                        is_partitioned = True
                        break
            elif isinstance(node, exp.Insert):
                if node.args.get("partition"):
                    is_partitioned = True

            # 4. Transpile to Spark SQL
            # sqlglot will automatically convert TEXT to STRING (if needed)

            sql_str = node.sql(dialect="hive", pretty=True)

            transformed_queries.append({
                'type': 'comment' if is_comment_only(node) else 'query',
                'content': sql_str
            })

        return JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            layer=context.layer,
            transformed_queries=transformed_queries,
            is_partitioned=is_partitioned,
            header_comments=formatted_header_comments  # Get header comment from context
        )