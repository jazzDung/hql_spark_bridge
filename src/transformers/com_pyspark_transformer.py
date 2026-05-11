import yaml
from sqlglot import exp
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.paths import VARIABLE_CONFIG_PATH
from src.transformers.utils import (
    is_comment_only,
    format_header_comments,
    replace_variables_in_node,
    replace_variables_in_strings,
    transform_outdated_com_raw_references
)

class ComPySparkTransformer(BaseSqlTransformer):
    """
    Transformer specifically designed for complex COM layer pre-processing steps in the migration module.
    Inherits from BaseSqlTransformer to allow custom and extensive AST manipulations.
    """
    
    def __init__(self, variable_mapping: dict = None):
        if variable_mapping is None:
            with open(VARIABLE_CONFIG_PATH, 'r', encoding='utf-8') as f:
                self.variable_mapping = yaml.safe_load(f)
        else:
            self.variable_mapping = variable_mapping

    def transform(self, context: SqlConversionContext, dialect: str = 'pyspark') -> JinjaRenderModel:
        transformed_queries = []
        is_partitioned = False

        # Handle header comments
        formatted_header_comments = format_header_comments(context.header_comments)

        for node in context.ast_nodes:
            # =====================================================================
            # 2. Implement complex, custom AST manipulation logic here!
            transform_outdated_com_raw_references(node, context)
            # =====================================================================

            # 1. Reuse base utilities for variables (Can be removed/changed if you want completely different logic)
            replace_variables_in_node(node, self.variable_mapping, dialect)
            replace_variables_in_strings(node, self.variable_mapping, dialect)

            # 3. Partition check
            if isinstance(node, exp.Create) and node.args.get("properties"):
                for prop in node.args["properties"].expressions:
                    if isinstance(prop, exp.PartitionedByProperty):
                        is_partitioned = True
                        break
            elif isinstance(node, exp.Insert):
                if node.args.get("partition"):
                    is_partitioned = True

            # 4. Transpile
            sql_str = node.sql(dialect="spark", pretty=True)

            transformed_queries.append({
                'type': 'comment' if is_comment_only(node) else 'query',
                'content': sql_str
            })

        return JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            transformed_queries=transformed_queries,
            is_partitioned=is_partitioned,
            header_comments=formatted_header_comments
        )
