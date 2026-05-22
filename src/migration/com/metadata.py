# src/migration/metadata.py

import yaml

from migration.schema_extractor import SchemaExtractor
from migration.com.decomposer import ComDecomposedScript

# Import từ các module khác thuộc Bước 2 (Step 2)
from src.migration.model_detector import ModelDetector
from migration.com.key_detector import ComKeyDetector
from src.migration.ddl_resolver import DdlResolver

from pathlib import Path


class ComMetadataProcessor:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules
        self.schema_extractor = SchemaExtractor(source_rules)
        self.model_detector = ModelDetector()
        self.key_detector = ComKeyDetector()

    def process(self, decomposed: ComDecomposedScript, input_path: Path, output_root: Path = None) -> dict:

        if output_root is None:
            output_root = Path("output/migration")

        # 1. Resolve DDL path
        ddl_resolver = DdlResolver(source_rules = self.source_rules)
        ddl_path = ddl_resolver.resolve_ddl_path(input_path)
        if not ddl_path:
            raise FileNotFoundError(f"Could not resolve DDL for {input_path}. Make sure DDL file exists.")

        # 2. Extract schema
        columns = self.schema_extractor.extract(ddl_path)

        # 3. Detect model type
        default_model = self.source_rules.get("default_model")
        model_type = self.model_detector.detect(decomposed.main_sql, source_default=default_model)

        # 4. Detect key
        primary_key = self.key_detector.detect(decomposed, self.source_rules)

        # 5. Build pre_processing blocks
        temp_table_rules = self.source_rules.get("temp_table_rules", {})
        pre_processing = []
        for temp_block in decomposed.temp_tables:
            action = "include"
            # find matching rule
            for rule_name, rule in temp_table_rules.items():
                if 'suffix' in rule:
                    if temp_block.name.endswith(rule.get("suffix", "")):
                        action = rule.get("action", "include")
                        break
            
            step_file_path = output_root / decomposed.pipeline_id / "processing_steps" / f"{decomposed.schema_name}_{temp_block.name}.sql"
            pre_processing.append({
                "name": temp_block.name,
                "file": str(step_file_path.as_posix()),
                "action": action,
                "dependencies": temp_block.dependencies
            })

        # 6. Calculate Delta columns
        delta_columns = [
            c["name"] for c in columns 
            if c["name"] not in primary_key['logical_primary_key'] and c.get("remark") != "non_original_field"
        ]

        target_table_name = f"t_{decomposed.source_name}_{decomposed.base_table}" if decomposed.sub_layer == "r" else decomposed.main_table
        main_file_path = output_root / decomposed.pipeline_id / "processing_steps" / "_main_dml.sql"
        header_comments_file_path = output_root / decomposed.pipeline_id / "processing_steps" / "_header_comments.sql"

        pipeline_config = {
            "pipeline_id": decomposed.pipeline_id,
            "layer": decomposed.schema_name,
            "model_type": model_type,
            "source_name": decomposed.source_name,
            "target_table_name": target_table_name,
            "columns": columns,
            "primary_key": primary_key,
            "header_comments": {
                "file": str(header_comments_file_path.as_posix())
            },
            "main_processing": {
                "file": str(main_file_path.as_posix())
            },
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