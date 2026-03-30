import re
from jinja2 import Environment, FileSystemLoader


# Function to automatically wrap {} around valid Python variables in SQL
def wrap_fstring_vars(sql_string):
    # Rule 1: Find all variables in the form params["..."] or params['...']
    # and wrap them as {params["..."]}
    sql_string = re.sub(r'(params\[["\'][a-zA-Z0-9_]+["\']\])', r'{\1}', sql_string)

    # Rule 2: (Optional) Find standalone variables like batch_date, last_date
    # Specific variables are listed to avoid accidentally wrapping lowercase words in SQL
    py_vars = ["batch_date", "last_date"]
    for var in py_vars:
        # Use \b regex to ensure exact word boundary matching, avoiding table names
        sql_string = re.sub(fr'\b({var})\b', r'{\1}', sql_string)

    return sql_string

# Function test:
# Input from sqlglot: CREATE TABLE params["raw_schema"].my_table (date STRING) LOCATION params["itl_data_path"]/batch_date
# Output: CREATE TABLE {params["raw_schema"]}.my_table (date STRING) LOCATION {params["itl_data_path"]}/{batch_date}