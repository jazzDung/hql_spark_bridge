from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import sqlglot.expressions as exp

@dataclass
class SqlConversionContext:
    """
    Chứa toàn bộ ngữ cảnh GỐC của một file HiveQL.
    Object này chỉ được ĐỌC (Read-only) bởi các Transformers.
    """
    # File thông tin cơ bản
    original_file_path: str
    raw_sql_content: str
    
    # Metadata bóc tách (Ví dụ bóc từ tên file hoặc LOCATION)
    source_name: str
    table_name: str

    # Lưu riêng phần header comment (thông tin file như Purpose, Author...)
    header_comments: str = ""
    
    # Cây AST (Abstract Syntax Tree) đã được parse bởi sqlglot
    ast_nodes: List[exp.Expression] = field(default_factory=list)
    
    # "Thùng rác" an toàn cho các config đặc thù nạp từ YAML (nếu cần)
    config_rules: Dict[str, Any] = field(default_factory=dict)

@dataclass
class JinjaRenderModel:
    """
    Chứa dữ liệu ĐẦU RA đã được xử lý xong, chuẩn bị đưa cho Jinja render.
    Mỗi loại script (Basic, Optimized) có thể có các fields khác nhau, 
    nhưng template jinja sẽ gọi chính xác các fields này.
    """
    source_name: str
    table_name: str
    
    # Danh sách các câu query đã được dịch sang dạng f-string
    # Ví dụ: ['DROP TABLE IF EXISTS {params["raw_schema"]}.my_table', 'CREATE TABLE...']
    transformed_queries: List[str] = field(default_factory=list)
    
    # Các biến bổ sung cho Template
    is_partitioned: bool = False
    
    # Chuyền header comment sang Jinja để in ra top của file Python
    header_comments: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert sang dict để **kwargs thẳng vào jinja_template.render()"""
        return self.__dict__
