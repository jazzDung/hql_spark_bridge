import os
from pathlib import Path


def parse_file_name(file_path: str | Path):
    # Extract basic metadata from filename (Assuming convention: raw_source_table.sql)
    file_suffix = Path(file_path).suffix
    filename = os.path.basename(file_path)
    base_name = Path(filename).stem

    if file_suffix == ".xml":
        parts = base_name.split('_', 1)
        layer = 'ext'
        sub_layer = None
        source_name = parts[0] if len(parts) >= 1 else "unknown"
        base_table = parts[1] if len(parts) >= 2 else base_name
        return layer, sub_layer, source_name, base_table

    parts = base_name.split('_', 3)
    layer = parts[0] if len(parts) >= 3 else "unknown"

    if layer == "raw":
        parts = base_name.split('_', 2)
        sub_layer = None
        source_name = parts[1] if len(parts) >= 2 else "unknown"
        base_table = parts[2] if len(parts) >= 2 else "unknown"
    elif layer == "com":
        sub_layer = parts[1] if len(parts) >= 3 else "unknown"
        source_name = parts[2] if len(parts) >= 3 else "unknown"
        base_table = parts[3] if len(parts) >= 3 else base_name
    elif layer == "unl":
        layer = f"{parts[0]}_{parts[1]}"
        sub_layer = None
        source_name = parts[2] if len(parts) >= 2 else "unknown"
        base_table = parts[3] if len(parts) >= 3 else base_name
    elif layer == "temp":
        sub_layer = parts[1] if len(parts) >= 3 else "unknown"
        source_name = parts[2] if len(parts) >= 3 else "unknown"
        base_table = parts[3] if len(parts) >= 3 else base_name
    else:
        sub_layer = None
        source_name = parts[1] if len(parts) >= 1 else "unknown"
        base_table = parts[2] if len(parts) >= 2 else base_name

    return layer, sub_layer, source_name, base_table


def read_conf_to_dict(file_path):
    conf_dict = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            # Xóa khoảng trắng 2 đầu
            line = line.strip()

            # Bỏ qua dòng trống hoặc dòng bắt đầu bằng '#' (nếu có comment)
            if not line or line.startswith('#'):
                continue

            # Tách key và value theo dấu '=' đầu tiên
            if '=' in line:
                key, value = line.split('=', 1)
                conf_dict[key.strip()] = value.strip()

    return conf_dict
