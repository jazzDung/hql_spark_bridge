import re
import sqlglot
from sqlglot import exp
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer

class BasicPySparkTransformer(BaseSqlTransformer):
    """
    Goal 1: Wrap the original SQL logic into PySpark (for easier BA readability).
    - Transform Hive configuration variables (${raw_schema}) into Python variables ({params["raw_schema"]}).
    - Convert dialect from hive -> spark.
    """
    
    def __init__(self, variable_mapping: dict):
        """
        Receives mapping rules from the variable.yaml file
        Example: {'raw_schema': 'params["raw_schema"]', 'batch_date': 'batch_date'}
        """
        self.variable_mapping = variable_mapping

    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        transformed_queries = []
        is_partitioned = False

        # --- Process Header Comments ---
        python_header_comments = []
        if context.header_comments:
            lines = context.header_comments.split('\n')
            # Filter out empty lines at the beginning/end of the comment block if any
            lines = [line for line in lines if line.strip()]

            if len(lines) > 1:
                # If it's a multi-line comment, use Python docstring
                python_header_comments.append('"""')
                for line in lines:
                    # Remove '--' and extra whitespace, then add to docstring
                    if line.strip().startswith('--'):
                        python_header_comments.append(line.strip()[2:].strip())
                    else:
                        python_header_comments.append(line.strip())
                python_header_comments.append('"""')
            elif len(lines) == 1:
                # If it's a single-line comment, use Python '#'
                line = lines[0]
                if line.strip().startswith('--'):
                    python_header_comments.append(f"# {line.strip()[2:].strip()}")
                else:
                    python_header_comments.append(f"# {line.strip()}")

        formatted_header_comments = '\n'.join(python_header_comments)
        # --- End of Header Comments Processing ---

        for node in context.ast_nodes:
            # 1. Scan the AST tree to replace variables (Parameter/Var)
            # In sqlglot, ${raw_schema} will be parsed as exp.Parameter(this=exp.Var(this="raw_schema"))
            for param_node in node.find_all(exp.Parameter):
                if isinstance(param_node.this, exp.Var):
                    var_name = param_node.this.name
                    if var_name in self.variable_mapping:
                        # Replace Parameter node with an Identifier containing the configuration string
                        # Example: replace with string '{params["raw_schema"]}'
                        py_var_str = f'{{{self.variable_mapping[var_name]}}}'
                        # Quoted=False so sqlglot doesn't wrap with backticks (`{params...}`)
                        param_node.replace(exp.Identifier(this=py_var_str, quoted=False))

            # 2. Process variables embedded in STRINGS (with single quotes), called Literal by sqlglot
            for literal_node in node.find_all(exp.Literal):
                if literal_node.is_string:
                    text_content = literal_node.this  # String content (excluding the two quotes)

                    # Rule A: If the entire string matches 100% with a variable that needs to be converted to a SQL Function
                    # Example: '${batch_timestamp}' -> current_timestamp()
                    exact_match = re.fullmatch(r'\$\{([a-zA-Z0-9_]+)\}', text_content)
                    if exact_match:
                        var_name = exact_match.group(1)
                        if var_name in self.variable_mapping:
                            mapped_val = self.variable_mapping[var_name]

                            # DECISION MOMENT: Identify whether this is a SQL Function or Python variable?
                            # Heuristic: If the configuration ends with '()' -> It's a SQL function
                            if mapped_val.endswith("()"):
                                # "Break" the single quotes by completely replacing the Literal node with a Function node
                                parsed_sql_expr = sqlglot.parse_one(mapped_val, read="spark")
                                literal_node.replace(parsed_sql_expr)
                                continue  # Done with this node, skip the steps below

                    # Rule B: If it's a Python variable (batch_date) embedded within a string
                    # Example: '/path/to/${batch_date}/file' -> '/path/to/{batch_date}/file'
                    def replace_fstring_var(match):
                        v_name = match.group(1)
                        if v_name in self.variable_mapping:
                            # Return f-string format
                            return f'{{{self.variable_mapping[v_name]}}}'
                        return match.group(0)

                    new_text = re.sub(r'\$\{([a-zA-Z0-9_]+)\}', replace_fstring_var, text_content)

                    # Update the string content (sqlglot will automatically wrap with two single quotes when generating code)
                    if new_text != text_content:
                        literal_node.args["this"] = new_text

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
            spark_sql_str = node.sql(dialect="spark", pretty=True)

            transformed_queries.append(spark_sql_str)

        return JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            transformed_queries=transformed_queries,
            is_partitioned=is_partitioned,
            header_comments=formatted_header_comments  # Get header comment from context
        )