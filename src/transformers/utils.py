import logging
import re
import sqlglot

from sqlglot import exp
from pathlib import Path
from src.core.ext_parser import ExtParser
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
# Import the centralized path for external XMLs
from src.paths import INCREMENATL_EXT_DIR


def is_comment_only(node):
    return (
        isinstance(node, exp.Semicolon)
        and not node.this
        and not node.args.get("expression")
    )


def extract_columns_from_create(node: exp.Create) -> list:
    """Extract the list of column names from a CREATE TABLE statement."""
    columns = []
    if node.args.get("this") and isinstance(node.args["this"], exp.Schema):
        for col_def in node.args["this"].expressions:
            if isinstance(col_def, exp.ColumnDef):
                columns.append(col_def.name)
    return columns


def format_header_comments(header_comments: str) -> str:
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


def replace_variables_in_node(node: exp.Expression, variable_mapping: dict, dialect: str) -> None:
    """
    Scan the AST tree to replace variables (Parameter/Var).
    In sqlglot, ${raw_schema} will be parsed as exp.Parameter(this=exp.Var(this="raw_schema"))
    """
    for param_node in node.find_all(exp.Parameter):
        if isinstance(param_node.this, exp.Var):
            var_name = param_node.this.name
            if var_name in variable_mapping:
                # Replace Parameter node with an Identifier containing the configuration string
                # Example: replace with string '{params["raw_schema"]}'
                py_var_str = f'{{{variable_mapping[var_name][dialect]}}}'
                # Quoted=False so sqlglot doesn't wrap with backticks (`{params...}`)
                param_node.replace(exp.Identifier(this=py_var_str, quoted=False))

def replace_variables_in_strings(node: exp.Expression, variable_mapping: dict, dialect: str) -> None:
    """
    Process variables embedded in STRINGS (with single quotes), called Literal by sqlglot
    """
    for literal_node in node.find_all(exp.Literal):
        if literal_node.is_string:
            text_content = literal_node.this  # String content (excluding the two quotes)

            # Rule A: If the entire string matches 100% with a variable that needs to be converted to a SQL Function
            # Example: '${batch_timestamp}' -> current_timestamp()
            exact_match = re.fullmatch(r'\$\{([a-zA-Z0-9_]+)\}', text_content)
            if exact_match:
                var_name = exact_match.group(1)
                if var_name in variable_mapping:
                    mapped_val = variable_mapping[var_name][dialect]

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
                if v_name in variable_mapping:
                    # Return f-string format
                    return f'{{{variable_mapping[v_name][dialect]}}}'
                return match.group(0)

            new_text = re.sub(r'\$\{([a-zA-Z0-9_]+)\}', replace_fstring_var, text_content)

            # Update the string content (sqlglot will automatically wrap with two single quotes when generating code)
            if new_text != text_content:
                literal_node.args["this"] = new_text

def transform_outdated_com_raw_references(node: exp.Expression, context: SqlConversionContext) -> None:
    """
    Transforms references to raw/com tables with outdated logic in the AST node.
    
    1. If a table reference has prefix "r_", change it to "t_".
    2. If the query references columns like "part_id":
       - If the table schema is raw_schema, replace the reference with etl_dt.
       - If the table schema is com_schema, replace the reference with dl_record_updated_date.
    """
    
    def extract_schema_name(db_node):
        if not db_node:
            return None
        if isinstance(db_node, exp.Parameter) and isinstance(db_node.this, exp.Var):
            return db_node.this.name
        name = getattr(db_node, "name", str(db_node))
        m1 = re.search(r'\$\{([^}]+)\}', name)
        if m1: 
            return m1.group(1)
        m2 = re.search(r'params\["([^"]+)"\]', name)
        if m2: 
            return m2.group(1)
        return name

    # 1. Parse all tables and map their alias/name to their schema
    alias_to_schema = {}
    for table_node in node.find_all(exp.Table):
        orig_name = table_node.name
        new_name = orig_name

        script_type = f"{getattr(context, 'layer', '')}_{getattr(context, 'sub_layer', '')}"

        # Handle table renaming (r_ to t_)
        if orig_name.startswith("r_"):
            if script_type == "com_temp":
                new_name = orig_name[2:]
                table_node.set("this", exp.Identifier(this=new_name, quoted=table_node.this.args.get("quoted", False)))
                table_node.set("db", exp.Parameter(this=exp.Var(this="raw_schema")))
            else:
                new_name = "t_" + orig_name[2:]
                table_node.set("this", exp.Identifier(this=new_name, quoted=table_node.this.args.get("quoted", False)))
            
        schema_name = extract_schema_name(table_node.args.get("db"))

        if schema_name:
            if table_node.alias:
                alias_to_schema[table_node.alias] = schema_name
            alias_to_schema[orig_name] = schema_name
            alias_to_schema[new_name] = schema_name

    # 2. Find and replace part_id references
    for op_node in node.find_all(exp.EQ, exp.LTE, exp.GTE, exp.LT, exp.GT):
        part_id_col = None
        if isinstance(op_node.left, exp.Column) and op_node.left.name.lower() == "part_id":
            part_id_col = op_node.left
        elif isinstance(op_node.right, exp.Column) and op_node.right.name.lower() == "part_id":
            part_id_col = op_node.right

        if part_id_col:
            schema_name = None
            if part_id_col.table:
                schema_name = alias_to_schema.get(part_id_col.table)
            else:
                for schema in alias_to_schema.values():
                    if schema in ("raw_schema", "com_schema"):
                        schema_name = schema
                        break

            # print(node.sql(dialect="give", pretty=True))
            # print(schema_name)

            if schema_name == "raw_schema":
                # Create alias.etl_dt
                new_col = exp.Column(
                    this=exp.Identifier(this="etl_dt"),
                    table=exp.Identifier(this=part_id_col.table) if part_id_col.table else None
                )
                
                # Replace the operator node with alias.etl_dt = '{batch_date}'
                new_op_node = exp.EQ(
                    this=new_col,
                    expression=exp.Literal(this="{batch_date}", is_string=True)
                )
                op_node.replace(new_op_node)

            elif schema_name == "com_schema":
                # Create TO_DATE(table_alias.dl_record_updated_date)
                col_expr = exp.Column(
                    this=exp.Identifier(this="dl_record_updated_date"),
                    table=exp.Identifier(this=part_id_col.table) if part_id_col.table else None
                )
                
                # We need to construct TO_DATE(...) cleanly using sqlglot
                new_left = exp.Anonymous(
                    this="TO_DATE",
                    expressions=[col_expr]
                )
                
                # Create the complex nested expression for the right side
                # TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{batch_date}', 'yyyyMMdd')))
                # Note: If batch_date has already been replaced by replace_variables_in_node
                # or replace_variables_in_strings, it might be {params["batch_date"]} or similar.
                # Assuming here the literal value on the right is what needs to be wrapped.
                
                # Get the value from the other side of the operator
                right_val_expr = op_node.right if part_id_col is op_node.left else op_node.left
                
                # Extract the string value if it's a literal or parameter
                date_val_str = ""
                if isinstance(right_val_expr, exp.Literal):
                    date_val_str = right_val_expr.this
                elif isinstance(right_val_expr, exp.Parameter) and isinstance(right_val_expr.this, exp.Var):
                    date_val_str = "${" + right_val_expr.this.name + "}"
                elif isinstance(right_val_expr, exp.Identifier):
                     # Could be an f-string already replaced
                     date_val_str = right_val_expr.name
                
                unix_ts = exp.Anonymous(
                    this="UNIX_TIMESTAMP",
                    expressions=[
                        exp.Literal.string(date_val_str),
                        exp.Literal.string("yyyyMMdd")
                    ]
                )
                
                from_unix = exp.Anonymous(
                    this="FROM_UNIXTIME",
                    expressions=[unix_ts]
                )
                
                new_right = exp.Anonymous(
                    this="TO_DATE",
                    expressions=[from_unix]
                )

                # Replace the entire operator node
                new_op_node = op_node.copy()
                if isinstance(new_op_node.left, exp.Column) and new_op_node.left.name.lower() == "part_id":
                    new_op_node.set("this", new_left)
                    new_op_node.set("expression", new_right)
                else:
                    new_op_node.set("expression", new_left)
                    new_op_node.set("this", new_right)

                op_node.replace(new_op_node)



def handle_skip_action(
    rule: dict,
    node: exp.Expression,
    context: SqlConversionContext,
) -> dict:
    """Handle the 'skip' action."""
    # Add logging here in the future if needed for traceability
    return {"type": "skip"}


def normalize_hive_create_properties(create_expr: exp.Create) -> exp.Create:
    """
    Remove all existing CREATE properties and replace them with:

    STORED AS PARQUET
    TBLPROPERTIES(
        'PARQUET.COMPRESSION'='SNAPPY',
        'EXTERNAL.TABLE.PURGE'='TRUE'
    )
    """

    new_properties = exp.Properties(
        expressions=[
            # STORED AS PARQUET
            exp.FileFormatProperty(
                this=exp.Var(this="PARQUET")
            ),

            # TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY')
            exp.Property(
                this=exp.Literal.string("PARQUET.COMPRESSION"),
                value=exp.Literal.string("SNAPPY"),
            ),

            # TBLPROPERTIES ('EXTERNAL.TABLE.PURGE'='TRUE')
            exp.Property(
                this=exp.Literal.string("EXTERNAL.TABLE.PURGE"),
                value=exp.Literal.string("TRUE"),
            ),
        ]
    )

    create_expr.set("properties", new_properties)

    return create_expr

def handle_replace_external_table_file_path_action(
    rule: dict,
    node: exp.Create,
    context: SqlConversionContext,
) -> dict:

    raw_reference_file_path = context.ext_context.raw_reference_file_path


    def update_location(node):
        if isinstance(node, exp.LocationProperty) and raw_reference_file_path is not None:
            return exp.LocationProperty(this=exp.Literal.string(raw_reference_file_path))
        return node

    # {itl_data_path}/fra_connected_parties_i.{batch_date}.dat', is_string=True)),
    # {params["itl_data_path"]}/{batch_date}/ConnectedParties_Data_{batch_date}.TXT

    return {
        "type": "replace_external_table_file_path",
        "file_name": Path(raw_reference_file_path).name,
        "external_table_create_node": node.transform(update_location)
    }



def handle_generate_jdbc_read_action(
    rule: dict,
    node: exp.Create,
    context: SqlConversionContext,
) -> dict:
    """
    Handle the 'generate_jdbc_read' action.
    
    This function now uses the centralized EXTERNAL_XML_DIR path to locate
    the required XML files, making the path resolution robust and portable.
    """
    action = rule.get("action", {})
    params = {**rule, **action.get("parameters", {})}
    columns = extract_columns_from_create(node)
    query = ""

    try:
        # Use the centrally defined path to build the full path to the XML file.
        ext_file_path = INCREMENATL_EXT_DIR / f"{context.source_name}_{context.table_name}.xml"
        
        # Add a check to ensure the file exists before trying to parse it.
        if not ext_file_path.exists():
            raise FileNotFoundError(f"External XML file not found at: {ext_file_path}")

        parser = ExtParser()
        parsed_ext = parser.parse_ext(ext_file_path)
        query = parsed_ext.ext_query_to_pyspark()

    except Exception as e:
        print(f"Error processing external XML file: {e}")
        print(f"Fallback to using default query template.")

        if "default_query_template" in params and columns:
            cols_str = ",\n    ".join(columns)
            query = params["default_query_template"].format(
                columns=cols_str,
                source_db_table=f"{params.get('schema', '')}.{context.table_name}"
            )
        else:
            # If no default template, the query will remain empty.\
            logging.warning("No default query template provided, using empty query.")

    return {
        "type": "generate_jdbc_read",
        "jdbc_url_variable": params.get("jdbc_url_variable"),
        "query": query,
        "source_db_table": f"{params.get('schema', '')}.{context.table_name}",
        "external_table_create_node": normalize_hive_create_properties(node)
    }


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


def is_rule_triggered(rule: dict, node: exp.Expression, context: SqlConversionContext) -> bool:
    """Check if the trigger conditions for a rule are met."""
    trigger = rule.get("trigger", {})
    condition_match_result = []

    for condition in trigger:
        if condition == "node_type":
            try:
                node_type = map_trigger_to_node_type(trigger["node_type"])
                if isinstance(node, node_type):
                    condition_match_result.append(True)
                else:
                    condition_match_result.append(False)
            except Exception:
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
                    raise ValueError(f"Unsupported node type for node: {node}")

                print(f"table_name: {table_name}, suffix: {suffixes}, result: {table_name.endswith(suffixes)}")

                if table_name.endswith(suffixes):
                    condition_match_result.append(True)
                else:
                    condition_match_result.append(False)
            except Exception:
                condition_match_result.append(False)

        if condition == "read_from_text_file":
            if context.ext_context is not None and context.ext_context.read_from_text_file is trigger["read_from_text_file"]:
                condition_match_result.append(True)
            else:
                condition_match_result.append(False)

    # print(f"condition_match_result for {type(node)}: {condition_match_result}, {all(condition_match_result)}")

    return all(condition_match_result)
