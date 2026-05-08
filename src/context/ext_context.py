import re
from dataclasses import dataclass

@dataclass
class ExtContext:
    """
    Context class to hold parsed information from Pentaho Kettle XML files.
    """
    source: str = None
    query: str = None
    read_from_text_file: bool = False
    output_file_path: str = None
    variable_mapping: dict = None
    source_db_config: dict = None

    def ext_query_to_pyspark(self) -> str:

        # Rule A: If the entire string matches 100% with a variable that needs to be converted to a SQL Function
        # Example: '${batch_timestamp}' -> current_timestamp()
        # exact_match = re.fullmatch(r'\$\{([a-zA-Z0-9_]+)\}', self.query)
        pattern = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

        def replacer(match):
            var_name = match.group(1)

            if var_name == 'db_schema':
                return self.source_db_config['src_schema']

            elif var_name not in self.variable_mapping:
                raise KeyError(f"Variable '{var_name}' not found")

            return f'{{{str(self.variable_mapping[var_name])}}}'

        return pattern.sub(replacer, self.query)
