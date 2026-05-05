from typing import Any
import re

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

def to_fstring(value: Any) -> str:
    """
    The filter will transform the string into:
    "SELECT {{col1, col2}} FROM my_table WHERE id = 1;"
    
    This is now a valid Python f-string that produces the original SQL.

    :param value: The input value (string or otherwise) to be escaped.
    :return: A string with f-string-escaped curly braces.
    """
    # Ensure the input is a string
    if not isinstance(value, str):
        value = str(value)

    # Escape f-string special characters by doubling them
    return value.replace("{", "{{").replace("}", "}}")

def comment_formatting(sql: str) -> str:
    """
    Split ONLY consecutive /* */ comments with nothing but whitespace between them.
    """

    # Pattern an toàn hơn
    pattern = re.compile(
        r'(/\*(?:(?!\*/).)*\*/(?:\s*/\*(?:(?!\*/).)*\*/)+)',
        re.DOTALL
    )

    def split_block(match):
        block = match.group(0)

        comments = re.findall(r'/\*.*?\*/', block, re.DOTALL)
        return "\n".join(to_fstring(c.strip()) for c in comments)

    result = pattern.sub(split_block, sql)

    return result

