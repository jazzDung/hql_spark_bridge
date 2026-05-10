import logging

from sqlglot import exp, expressions
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
                    raise ValueError(f"Unsupported node type for node: {node}")

                print(f"table_name: {table_name}, suffix: {suffixes}, result: {table_name.endswith(suffixes)}")

                if table_name.endswith(suffixes):
                    condition_match_result.append(True)
                else:
                    condition_match_result.append(False)
            except:
                condition_match_result.append(False)

        if condition == "read_from_text_file":
            if context.ext_context.read_from_text_file is trigger["read_from_text_file"]:
                condition_match_result.append(True)
            else:
                condition_match_result.append(False)

    # print(f"condition_match_result for {type(node)}: {condition_match_result}, {all(condition_match_result)}")

    return all(condition_match_result)
