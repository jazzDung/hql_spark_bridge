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


def replace_variables_in_node(node: exp.Expression, variable_mapping: dict, dialect: str = 'pyspark') -> exp.Expression:
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
    
    return node

def replace_variables_in_strings(node: exp.Expression, variable_mapping: dict, dialect: str = 'pyspark') -> exp.Expression:
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

    return node

def replace_variables_in_comments(node: exp.Expression, variable_mapping: dict, dialect: str = 'pyspark') -> exp.Expression:
    """
    Quét và thay thế các biến (dạng ${var_name}) nằm riêng trong comments của cây AST sqlglot.
    """
    # generator .walk() duyệt qua mọi node trong AST
    for n in node.walk():
        # Kiểm tra node có mang thuộc tính comments không
        if hasattr(n, "comments") and n.comments:
            new_comments = []
            for comment_text in n.comments:

                def replace_comment_var(match):
                    v_name = match.group(1)
                    if v_name in variable_mapping:
                        mapped = variable_mapping[v_name][dialect]
                        # Trong comment thì text là text thuần, không phải AST node
                        # Nếu là hàm SQL thì trả về chuỗi hàm, nếu là biến Python thì bọc ngoặc nhọn
                        return mapped if mapped.endswith("()") else f'{{{mapped}}}'
                    return match.group(0)

                updated_comment = re.sub(r'\$\{([a-zA-Z0-9_]+)\}', replace_comment_var, comment_text)
                new_comments.append(updated_comment)

            # Ghi đè lại mảng comment của node
            n.comments.clear()
            # Bọc thêm \n ở đầu và cuối để format block comment trông đẹp mắt hơn
            merged_comment = "\n".join(new_comments)
            n.comments.append(merged_comment)

    return node


import sqlglot
from sqlglot import exp


def remove_part_id_from_projections(expression: exp.Expression) -> exp.Expression:
    """
    Bước 1: Loại bỏ sự xuất hiện của 'part_id' trong các mệnh đề định nghĩa/khai báo.
    - Cột trong lệnh SELECT.
    - Định nghĩa cột trong lệnh CREATE TABLE.
    """

    def transformer(node):
        if isinstance(node, exp.Select):
            new_exprs = []
            for e in node.expressions:
                # Dùng .unalias() để bóc tách trường hợp alias (VD: part_id AS p)
                col = e.unalias()
                # Kiểm tra nếu biểu thức cốt lõi là Column và tên là part_id
                if isinstance(col, exp.Column) and col.name.lower() == "part_id":
                    continue  # Bỏ qua, không đưa vào danh sách mới
                new_exprs.append(e)

            # Trả về node mới với danh sách expressions đã được lọc
            new_node = node.copy()
            new_node.set("expressions", new_exprs)
            return new_node

        elif isinstance(node, exp.Schema):
            # Xử lý cho CREATE TABLE (exp.Schema chứa danh sách các exp.ColumnDef)
            new_exprs = []
            for e in node.expressions:
                if isinstance(e, exp.ColumnDef) and e.name.lower() == "part_id":
                    continue
                new_exprs.append(e)

            new_node = node.copy()
            new_node.set("expressions", new_exprs)
            return new_node

        return node

    # Copy=True để đảm bảo an toàn không thay đổi trực tiếp cây gốc trong lúc duyệt
    return expression.copy().transform(transformer)


def rename_remaining_identifiers_and_tables(expression: exp.Expression) -> exp.Expression:
    """
    Bước 2: Xử lý các node còn sót lại trong AST (lúc này part_id chỉ còn nằm ở
    JOIN, WHERE, PARTITION BY, ORDER BY...).
    Đồng thời xử lý đổi tên bảng tiền tố r_.
    """

    def transformer(node):
        # LUẬT 1: Đổi tên bảng r_
        if isinstance(node, exp.Table):
            table_name = node.name
            if table_name and table_name.lower().startswith("r_"):
                new_node = node.copy()
                new_node.this.set("this", table_name[2:])
                new_db = exp.Parameter(this=exp.Var(this="raw_schema"), expression=False)
                new_node.set("db", new_db)
                return new_node

        # LUẬT 2: Đổi part_id -> etl_dt
        if isinstance(node, exp.Identifier):
            if node.name.lower() == "part_id":
                new_node = node.copy()
                new_node.set("this", "etl_dt")
                return new_node

        return node

    return expression.transform(transformer)


def replace_table_identifier(
    node: exp.Expression,
    old_schema: str, old_table: str,
    new_schema: str, new_table: str,
    dialect: str = "hive"
) -> exp.Expression:
    """
    Thay thế chính xác 1 bảng trong AST. Đã vá lỗi ép kiểu (into=exp.Table).
    """
    # ÉP KIỂU: Bắt buộc parse chuỗi dưới dạng Table thay vì Column
    old_expr = sqlglot.parse_one(f"{old_schema}.{old_table}", read=dialect, into=exp.Table)
    new_expr = sqlglot.parse_one(f"{new_schema}.{new_table}", read=dialect, into=exp.Table)

    # Lúc này old_expr chắc chắn là Table, lấy db an toàn
    old_db_norm = old_expr.args.get("db").sql(dialect) if old_expr.args.get("db") else ""
    old_tbl_norm = old_expr.name.lower()

    def transformer(n):
        if isinstance(n, exp.Table):
            curr_db_norm = n.args.get("db").sql(dialect) if n.args.get("db") else ""
            curr_tbl_norm = n.name.lower()

            if curr_db_norm == old_db_norm and curr_tbl_norm == old_tbl_norm:
                new_n = n.copy()
                new_n.set("this", new_expr.args.get("this").copy())

                if new_expr.args.get("db"):
                    new_n.set("db", new_expr.args.get("db").copy())
                else:
                    new_n.args.pop("db", None)
                return new_n
        return n

    return node.transform(transformer)

def strip_partition_clauses(node: exp.Expression) -> exp.Expression:
    """
    Quét toàn bộ AST và loại bỏ mọi cấu trúc PARTITION(...).
    Hoạt động tốt với INSERT, CREATE TABLE, v.v.
    """
    def transformer(n):
        # Nếu node là định dạng PARTITION(...)
        if isinstance(n, exp.Partition):
            return None # Báo cho sqlglot xóa node này khỏi AST
        return n

    # Tạo bản sao để không làm hỏng AST gốc nếu cần dùng lại
    return node.copy().transform(transformer)

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
