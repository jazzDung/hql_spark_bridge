from sqlglot import exp, expressions

from src.core.ext_parser import ExtParser
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel


def _extract_columns_from_create(node: exp.Create) -> list:
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


def _handle_skip_action(rule: dict, node: exp.Expression, context: SqlConversionContext) -> dict:
    """Handle the 'skip' action."""
    # Add logging here in the future if needed for traceability

    return {"type": "skip"}


def handle_generate_jdbc_read_action(config_root, rule: dict, node: exp.Expression, context: SqlConversionContext) -> dict:
    """Handle the 'generate_jdbc_read' action with robustness checks."""
    action = rule.get("action", {})
    params = {**rule, **action.get("parameters", {})}
    columns = _extract_columns_from_create(node)

    query = ""
    if "query_template" in params and columns:
        cols_str = ",\n    ".join(columns)
        query = params["query_template"].format(
            columns=cols_str,
            source_db_table=f"{params.get('schema', '')}.{context.table_name}"
        )
    else:
        ext_file_path = config_root.parent / "samples" / "input" / "ext" / "xml" / f"{context.source_name}_{context.table_name}.xml"
        parsed_ext = ExtParser.parse_ext(ext_file_path)
        query = parsed_ext.query
        # query = params.get("query", "")

    return {
        "type": "jdbc_read",
        "jdbc_url_variable": params.get("jdbc_url_variable"),
        "query": query,
        "source_db_table": f"{params.get('schema', '')}.{context.table_name}"
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
