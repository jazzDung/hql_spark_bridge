import re
from jinja2 import Environment, FileSystemLoader


# Hàm tự động bọc {} quanh các biến Python hợp lệ trong SQL
def wrap_fstring_vars(sql_string):
    # Rule 1: Tìm tất cả các biến dạng params["..."] hoặc params['...']
    # và bọc chúng lại thành {params["..."]}
    sql_string = re.sub(r'(params\[["\'][a-zA-Z0-9_]+["\']\])', r'{\1}', sql_string)

    # Rule 2: (Tùy chọn) Tìm các biến độc lập như batch_date, last_date
    # Bạn có thể liệt kê cụ thể để tránh bọc nhầm chữ thường trong SQL
    py_vars = ["batch_date", "last_date"]
    for var in py_vars:
        # Regex \b để đảm bảo khớp đúng từ (word boundary), không khớp nhầm vào tên bảng
        sql_string = re.sub(fr'\b({var})\b', r'{\1}', sql_string)

    return sql_string


# Thử nghiệm hàm:
# Đầu vào từ sqlglot: CREATE TABLE params["raw_schema"].my_table (date STRING) LOCATION params["itl_data_path"]/batch_date
# Đầu ra: CREATE TABLE {params["raw_schema"]}.my_table (date STRING) LOCATION {params["itl_data_path"]}/{batch_date}