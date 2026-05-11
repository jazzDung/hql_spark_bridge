import os
import re
import sqlglot
from src.context.sql_conversion_context import SqlConversionContext
from src.utils.file_utils import parse_file_name

class HiveScriptParser:
    """
    Responsible for reading physical SQL files and parsing them into AST via sqlglot.
    Returns a single SqlConversionContext object (Single Source of Truth).
    """
    
    @staticmethod
    def parse_file(file_path: str) -> SqlConversionContext:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()

        layer, sub_layer, source_name, base_table = parse_file_name(file_path)

        # Extract Header Comments (consecutive lines starting with -- at the beginning of the file)
        header_lines = []
        sql_lines = []
        is_header = True
        
        for line in raw_content.split('\n'):
            stripped_line = line.strip()
            # If it's a comment and within the initial block
            if is_header and (stripped_line.startswith('--') or not stripped_line):
                header_lines.append(line)
            else:
                is_header = False
                sql_lines.append(line)
        
        header_comments = '\n'.join(header_lines).strip()
        raw_sql_without_header = '\n'.join(sql_lines)

        # Pre-processing: Remove non-standard Hive commands that sqlglot cannot parse
        # Example: source /path/to/script.sql;
        cleaned_lines = []
        for line in raw_sql_without_header.split('\n'):
            if line.strip().lower().startswith('source '):
                continue
                # Convert to comment to keep context if needed, without breaking the parser
                # cleaned_lines.append(f"-- Ignored non-standard command: {line}")
            else:
                cleaned_lines.append(line)
        
        cleaned_content = '\n'.join(cleaned_lines)
        
        # Extract basic metadata from filename (Assuming convention: raw_source_table.sql)
        filename = os.path.basename(file_path)
        base_name = filename.replace('.sql', '')
        
        parts = base_name.split('_', 2)
        source_name = parts[1] if len(parts) >= 2 else "unknown"
        table_name = parts[2] if len(parts) >= 3 else base_name
        
        # Parse into AST
        try:
            ast_nodes = sqlglot.parse(cleaned_content, read="hive")
        except Exception as e:
            raise ValueError(f"Error parsing file {filename} with sqlglot: {str(e)}")

        return SqlConversionContext(
            original_file_path=file_path,
            raw_sql_content=cleaned_content,
            header_comments=header_comments,
            source_name=source_name,
            table_name=table_name,
            layer=layer,
            sub_layer=sub_layer,
            ast_nodes=[node for node in ast_nodes if node is not None]
        )