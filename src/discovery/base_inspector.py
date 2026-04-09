from abc import ABC, abstractmethod
import json
import os
from pathlib import Path


class BaseInspector(ABC):
    def __init__(self, spark_session, conn_config):
        self.spark = spark_session
        self.config = conn_config

    @abstractmethod
    def get_schema(self, table_name):
        """Trả về StructType hoặc Dict mô tả schema"""
        pass

    @abstractmethod
    def get_sample_query(self, table_name):
        """Trả về câu lệnh SELECT mặc định"""
        pass

    def save_metadata(self, source_name: str, table_name: str, schema_dict, query_sql, schema_output_path: str, query_output_path: str):
        """Lưu trữ tập trung vào folder được chỉ định"""
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(schema_output_path, exist_ok=True)
        os.makedirs(query_output_path, exist_ok=True)
        output_file_name = f"{source_name.lower()}_{table_name.lower()}"
        
        schema_file = Path(schema_output_path) / f"{output_file_name}.json"
        query_file = Path(query_output_path) / f"{output_file_name}.sql"

        with open(schema_file, "w") as f:
            json.dump(schema_dict, f, indent=4)

        with open(query_file, "w") as f:
            f.write(query_sql)
