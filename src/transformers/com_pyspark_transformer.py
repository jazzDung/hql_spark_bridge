from typing import Dict, Any

import yaml
from IPython.lib import pretty
from sqlglot import exp
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer
from src.paths import VARIABLE_CONFIG_PATH
from src.transformers.utils import *

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

    def transform(self, pipeline_config: Dict[str, Any], context: SqlConversionContext, dialect: str = 'pyspark') -> JinjaRenderModel:
        transformed_queries = []
        is_partitioned = False

        # Handle header comments
        formatted_header_comments = format_header_comments(context.header_comments)

        for node in context.ast_nodes:
            # =====================================================================
            # 2. Implement complex, custom AST manipulation logic here!
            node = remove_part_id_from_projections(node)
            node = rename_remaining_identifiers_and_tables_for_com(node)
            # =====================================================================

            # Special processing for main sql
            if context.source_name == 'main':
                target_table_name = pipeline_config["target_table_name"]

                node = replace_table_identifier(node,
                    "${com_schema}", target_table_name,
                    "${com_schema}", f"temp_{target_table_name}_consolidated")

                node = remove_non_standard_fields(node)

                node = strip_partition_clauses(node)

            # 1. Reuse base utilities for variables (Can be removed/changed if you want completely different logic)
            node = replace_variables_in_node(node, self.variable_mapping, dialect)
            node = replace_variables_in_strings(node, self.variable_mapping, dialect)
            node = replace_variables_in_comments(node, self.variable_mapping, dialect)




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
            layer=context.layer,
            source_name=context.source_name,
            table_name=context.table_name,
            transformed_queries=transformed_queries,
            is_partitioned=is_partitioned,
            header_comments=formatted_header_comments
        )
