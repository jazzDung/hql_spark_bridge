import re
import sqlglot
from sqlglot import exp
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel
from src.transformers.base_transformer import BaseSqlTransformer

class BasicPySparkTransformer(BaseSqlTransformer):
    """
    Mục tiêu 1: Bọc nguyên logic SQL cũ thành PySpark (cho BA dễ đọc).
    - Biến đổi biến cấu hình Hive (${raw_schema}) thành biến Python ({params["raw_schema"]}).
    - Chuyển dialect từ hive -> spark.
    """
    
    def __init__(self, variable_mapping: dict):
        """
        Nhận vào mapping rule từ file variable.yaml
        Ví dụ: {'raw_schema': 'params["raw_schema"]', 'batch_date': 'batch_date'}
        """
        self.variable_mapping = variable_mapping

    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        transformed_queries = []
        is_partitioned = False

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
            # 1. Quét cây AST để thay thế biến (Parameter/Var)
            # Trong sqlglot, ${raw_schema} sẽ được parse thành exp.Parameter(this=exp.Var(this="raw_schema"))
            for param_node in node.find_all(exp.Parameter):
                if isinstance(param_node.this, exp.Var):
                    var_name = param_node.this.name
                    if var_name in self.variable_mapping:
                        # Thay thế node Parameter bằng một Identifier chứa chuỗi cấu hình
                        # Ví dụ: thay bằng chuỗi '{params["raw_schema"]}'
                        py_var_str = f'{{{self.variable_mapping[var_name]}}}'
                        # Quoted=False để sqlglot không bọc dấu backtick (`{params...}`)
                        param_node.replace(exp.Identifier(this=py_var_str, quoted=False))

            # 2. Xử lý các biến bị nhốt trong CHUỖI (có nháy đơn), sqlglot gọi là Literal
            for literal_node in node.find_all(exp.Literal):
                if literal_node.is_string:
                    text_content = literal_node.this  # Nội dung chuỗi (không bao gồm 2 dấu nháy)

                    # Rule A: Nếu toàn bộ chuỗi khớp 100% với một biến cần chuyển thành Hàm SQL
                    # Ví dụ: '${batch_timestamp}' -> current_timestamp()
                    exact_match = re.fullmatch(r'\$\{([a-zA-Z0-9_]+)\}', text_content)
                    if exact_match:
                        var_name = exact_match.group(1)
                        if var_name in self.variable_mapping:
                            mapped_val = self.variable_mapping[var_name]

                            # KHOẢNH KHẮC QUYẾT ĐỊNH: Nhận diện đây là Hàm SQL hay biến Python?
                            # Heuristic: Nếu cấu hình kết thúc bằng '()' -> Nó là hàm SQL
                            if mapped_val.endswith("()"):
                                # "Phá" bỏ nháy đơn bằng cách đè nguyên node Literal thành node Function
                                parsed_sql_expr = sqlglot.parse_one(mapped_val, read="spark")
                                literal_node.replace(parsed_sql_expr)
                                continue  # Xong node này, bỏ qua các bước dưới

                    # Rule B: Nếu là biến Python (batch_date) nằm xen kẽ trong chuỗi
                    # Ví dụ: '/path/to/${batch_date}/file' -> '/path/to/{batch_date}/file'
                    def replace_fstring_var(match):
                        v_name = match.group(1)
                        if v_name in self.variable_mapping:
                            # Trả về format của f-string
                            return f'{{{self.variable_mapping[v_name]}}}'
                        return match.group(0)

                    new_text = re.sub(r'\$\{([a-zA-Z0-9_]+)\}', replace_fstring_var, text_content)

                    # Cập nhật lại nội dung chuỗi (sqlglot sẽ tự động bọc lại 2 dấu nháy đơn khi sinh code)
                    if new_text != text_content:
                        literal_node.args["this"] = new_text


            # 3. Kiểm tra xem bảng có chia partition không (dành cho Jinja để biết có gọi lệnh drop_partition không)
            if isinstance(node, exp.Create) and node.args.get("properties"):
                for prop in node.args["properties"].expressions:
                    if isinstance(prop, exp.PartitionedByProperty):
                        is_partitioned = True
                        break
            elif isinstance(node, exp.Insert):
                if node.args.get("partition"):
                    is_partitioned = True

            # 4. Transpile thành Spark SQL
            # sqlglot sẽ tự động chuyển TEXT thành STRING (nếu cần)
            spark_sql_str = node.sql(dialect="spark", pretty=True)

            transformed_queries.append(spark_sql_str)

        return JinjaRenderModel(
            source_name=context.source_name,
            table_name=context.table_name,
            transformed_queries=transformed_queries,
            is_partitioned=is_partitioned,
            header_comments=formatted_header_comments # Lấy header comment từ context
        )