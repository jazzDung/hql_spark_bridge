# src/migration/metadata.py

from pathlib import Path
from typing import Optional
import yaml
import re

from src.migration.decomposer import DecomposedScript
from src.core.ddl_parser import DDLParser

# Import từ các module khác thuộc Bước 2 (Step 2)
from src.migration.model_detector import ModelDetector
from src.migration.key_detector import KeyDetector

def _file_has_create_table(path: Path) -> bool:
    try:
        content = path.read_text(encoding="utf-8").upper()
        return "CREATE TABLE" in content
    except Exception:
        return False

def resolve_ddl_path(dml_path: Path) -> Optional[Path]:
    """
    DDL resolution priority:
    1. Same directory, same stem, same extension (DDL file contains 'CREATE TABLE' as first statement)
    2. Sibling directory named 'ddl/' with same stem
    3. Parent's sibling directory named 'ddl/' with same stem
    """
    candidates = [
        # dml_path.parent / dml_path.name,              # same file (multi-statement)
        # dml_path.parent / "ddl" / dml_path.name,      # sibling dir
        dml_path.parent.parent / "ddl" / dml_path.name, # parent's sibling dir
    ]

    # print(candidates)
    for c in candidates:
        if c.exists() and _file_has_create_table(c):
            return c
    return None

class SchemaExtractor:
    def __init__(self, source_rules: dict):
        self.non_original_fields = set(
            source_rules.get("column_rules", {})
                        .get("non_original_fields", {})
                        .get("name", [])
        )
        self.ddl_parser = DDLParser() # uses default config

    def extract(self, ddl_path: Path) -> list[dict]:
        ddl_content = ddl_path.read_text(encoding="utf-8")
        
        # Simple extraction for SOURCE statements removal before passing to parser
        ddl_content = re.sub(r'source\s+\S+;', '', ddl_content, flags=re.IGNORECASE)

        full_table_name, struct_type = self.ddl_parser.parse_hive_ddl(ddl_content)
        
        result = []
        for field in struct_type.fields:
            col_name = field.name.lower()
            remark = "non_original_field" if col_name in self.non_original_fields else None
            # DataType.simpleString() returns strings like "string", "decimal(18,2)"
            result.append({
                "name": col_name,
                "type": field.dataType.simpleString().upper(),
                "remark": remark
            })


        return result

class MetadataProcessor:
    def __init__(self, source_rules: dict, ai_fallback: bool = False):
        self.source_rules = source_rules
        self.ai_fallback = ai_fallback
        self.schema_extractor = SchemaExtractor(source_rules)
        self.model_detector = ModelDetector()
        self.key_detector = KeyDetector(ai_fallback=ai_fallback)

    def process(self, decomposed: DecomposedScript, input_path: Path, output_root: Path) -> dict:
        # 1. Resolve DDL path
        ddl_path = resolve_ddl_path(input_path)
        if not ddl_path:
            raise FileNotFoundError(f"Could not resolve DDL for {input_path}. Make sure DDL file exists.")

        # 2. Extract schema
        columns = self.schema_extractor.extract(ddl_path)

        # 3. Detect model type
        default_model = self.source_rules.get("default_model")
        model_type = self.model_detector.detect(decomposed.main_sql, source_default=default_model)

        # 4. Detect key
        key = self.key_detector.detect(decomposed, columns, self.source_rules)

        # 5. Build pre_processing blocks
        temp_table_rules = self.source_rules.get("temp_table_rules", {})
        pre_processing = []
        for temp_block in decomposed.temp_tables:
            action = "include"
            # find matching rule
            for rule_name, rule in temp_table_rules.items():
                if temp_block.name.endswith(rule.get("suffix", "")):
                    action = rule.get("action", "include")
                    break
            
            step_file_path = output_root / decomposed.pipeline_id / "processing_steps" / decomposed.base_table / f"{temp_block.name}.sql"
            pre_processing.append({
                "name": temp_block.name,
                "file": str(step_file_path.as_posix()),
                "action": action
            })

        # 6. Calculate Delta columns
        delta_columns = [
            c["name"] for c in columns 
            if c["name"] != key and c.get("remark") != "non_original_field"
        ]

        target_table_name = f"t_{decomposed.source_name}_{decomposed.base_table}" if decomposed.sub_layer == "r" else decomposed.main_table

        pipeline_config = {
            "pipeline_id": decomposed.pipeline_id,
            "layer": decomposed.layer,
            "model_type": model_type,
            "source_name": decomposed.source_name,
            "target_table_name": target_table_name,
            "columns": columns,
            "key": key,
            "key_detection_strategy": "auto",  # This can be extracted if key_detector returns a tuple
            "pre_processing": pre_processing,
            "delta_columns": delta_columns,
            "applied_source_rule": self.source_rules.get("source", "unknown"),
            "ddl_source": str(ddl_path.as_posix())
        }
        return pipeline_config

    def write_yaml(self, pipeline_config: dict, output_dir: Path):
        output_dir.mkdir(parents=True, exist_ok=True)
        out_file = output_dir / f"{pipeline_config['pipeline_id']}.yaml"
        with open(out_file, "w", encoding="utf-8") as f:
            yaml.dump(pipeline_config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
