import os
import yaml
from pathlib import Path
from typing import Dict, Any
from src.paths import *

def load_all_source_rules(rules_dir: Path | str = None) -> Dict[str, Any]:
    """
    Đọc tất cả các file YAML trong thư mục chỉ định và trả về một dictionary.
    Key là tên file (không có phần mở rộng .yaml), Value là nội dung đã được parse thành dict.
    """

    if rules_dir is None:
        rules_dir_path = CONFIGS_DIR / "rules" / "migration"
    else:
        rules_dir_path = Path(rules_dir)

    source_rules = {}

    if not rules_dir_path.exists() or not rules_dir_path.is_dir():
        print(f"Thư mục {rules_dir_path} không tồn tại hoặc không phải là thư mục.")
        return source_rules

    for file_path in rules_dir_path.glob("*.yaml"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                # Dùng stem để lấy tên file không kèm đuôi .yaml
                source_rules[file_path.stem] = content or {}
        except yaml.YAMLError as e:
            print(f"Lỗi cú pháp YAML khi đọc file {file_path.name}: {e}")
        except Exception as e:
            print(f"Lỗi khi đọc file {file_path.name}: {e}")

    return source_rules

def load_all_layer_extra_fields(rules_dir: Path | str = None) -> Dict[str, Any]:
    """
    Đọc tất cả các file YAML trong thư mục chỉ định và trả về một dictionary.
    Key là tên file (không có phần mở rộng .yaml), Value là nội dung đã được parse thành dict.
    """

    if rules_dir is None:
        rules_dir_path = CONFIGS_DIR / "rules" / "extra_fields"
    else:
        rules_dir_path = Path(rules_dir)

    extra_fields = {}

    if not rules_dir_path.exists() or not rules_dir_path.is_dir():
        print(f"Thư mục {rules_dir_path} không tồn tại hoặc không phải là thư mục.")
        return extra_fields

    for file_path in rules_dir_path.glob("*.yaml"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)
                # Dùng stem để lấy tên file không kèm đuôi .yaml
                extra_fields[file_path.stem] = content or {}
        except yaml.YAMLError as e:
            print(f"Lỗi cú pháp YAML khi đọc file {file_path.name}: {e}")
        except Exception as e:
            print(f"Lỗi khi đọc file {file_path.name}: {e}")

    return extra_fields
