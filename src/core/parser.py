import os
import re
import sqlglot
from src.context.sql_conversion_context import SqlConversionContext

class HiveScriptParser:
    """
    Chịu trách nhiệm đọc file SQL vật lý và parse thành AST thông qua sqlglot.
    Trả về một đối tượng SqlConversionContext duy nhất (Single Source of Truth).
    """
    
    @staticmethod
    def parse_file(file_path: str) -> SqlConversionContext:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
            
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()

        # Bóc tách Header Comment (các dòng bắt đầu bằng -- liên tục ở đầu file)
        header_lines = []
        sql_lines = []
        is_header = True
        
        for line in raw_content.split('\n'):
            stripped_line = line.strip()
            # Nếu là comment và đang ở khối đầu tiên
            if is_header and (stripped_line.startswith('--') or not stripped_line):
                header_lines.append(line)
            else:
                is_header = False
                sql_lines.append(line)
        
        header_comments = '\n'.join(header_lines).strip()
        raw_sql_without_header = '\n'.join(sql_lines)

        # Tiền xử lý (Pre-processing): Loại bỏ các lệnh Hive không chuẩn mà sqlglot không hiểu
        # Ví dụ: source /path/to/script.sql;
        cleaned_lines = []
        for line in raw_sql_without_header.split('\n'):
            if line.strip().lower().startswith('source '):
                continue
                # Biến nó thành comment để giữ lại context nếu cần, nhưng không làm vỡ parser
                # cleaned_lines.append(f"-- Bỏ qua lệnh không chuẩn: {line}")
            else:
                cleaned_lines.append(line)
        
        cleaned_content = '\n'.join(cleaned_lines)
        
        # Bóc tách metadata cơ bản từ tên file (Giả định quy tắc: raw_source_table.sql)
        filename = os.path.basename(file_path)
        base_name = filename.replace('.sql', '')
        
        parts = base_name.split('_', 2)
        source_name = parts[1] if len(parts) >= 2 else "unknown"
        table_name = parts[2] if len(parts) >= 3 else base_name
        
        # Parse thành AST
        try:
            ast_nodes = sqlglot.parse(cleaned_content, read="hive")
        except Exception as e:
            raise ValueError(f"Lỗi khi parse file {filename} bằng sqlglot: {str(e)}")

        return SqlConversionContext(
            original_file_path=file_path,
            raw_sql_content=cleaned_content,
            header_comments=header_comments,
            source_name=source_name,
            table_name=table_name,
            ast_nodes=[node for node in ast_nodes if node is not None]
        )