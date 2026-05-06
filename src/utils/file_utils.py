import os
from pathlib import Path


def parse_file_name(file_path: str | Path):
    # Extract basic metadata from filename (Assuming convention: raw_source_table.sql)
    filename = os.path.basename(file_path)
    base_name = filename.replace('.sql', '')
    parts = base_name.split('_', 3)
    layer = parts[0] if len(parts) >= 3 else "unknown"

    if layer == "com":
        sub_layer = parts[1] if len(parts) >= 3 else "unknown"
        source_name = parts[2] if len(parts) >= 3 else "unknown"
        base_table = parts[3] if len(parts) >= 3 else base_name
    elif layer == "unl":
        layer = f"{parts[0]}_{parts[1]}"
        sub_layer = None
        source_name = parts[2] if len(parts) >= 2 else "unknown"
        base_table = parts[3] if len(parts) >= 3 else base_name
    else:
        sub_layer = None
        source_name = parts[1] if len(parts) >= 1 else "unknown"
        base_table = parts[2] if len(parts) >= 2 else base_name

    return layer, sub_layer, source_name, base_table
