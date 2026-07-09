import yaml
from pathlib import Path

from migration.cur.decomposer import CurDecomposedScript
from src.migration.model_detector import ModelDetector
from src.migration.cur.key_detector import CurKeyDetector
from src.migration.ddl_resolver import DdlResolver
from src.migration.schema_extractor import SchemaExtractor


class CurMetadataProcessor:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules
        self.schema_extractor = SchemaExtractor(source_rules)
        self.model_detector = ModelDetector()
        # Cur schema nên dùng bản V2 để detect key chính xác hơn dựa trên window function
        self.key_detector = CurKeyDetector()

    def process(self, decomposed: CurDecomposedScript, input_path: Path, output_root: Path = None) -> dict:

        if output_root is None:
            output_root = Path("output/migration")

        # 1. Resolve DDL path
        ddl_resolver = DdlResolver(source_rules=self.source_rules)
        ddl_path = ddl_resolver.resolve_ddl_path(input_path)

        if not ddl_path:
            print(input_path.parent / f"cur_{decomposed.target_table.lower()}.sql")
            ddl_path = ddl_resolver.resolve_ddl_path(input_path.parent / f"cur_{decomposed.target_table.lower()}.sql")

        if not ddl_path:
            raise FileNotFoundError(f"Could not resolve DDL for {input_path}. Make sure DDL file exists.")

        # 2. Extract schema
        columns = self.schema_extractor.extract(ddl_path)

        # 3. Detect model type
        default_model = self.source_rules.get("default_model")
        model_type = self.model_detector.detect(decomposed.main_sql, source_default=default_model)

        # 4. Detect key
        primary_key = self.key_detector.detect(decomposed, self.source_rules)

        # 5. Build processing blocks (Source Blocks instead of Temp Tables)
        processing_steps = []
        for source_id, block in decomposed.source_blocks.items():
            
            # Determine file name matching CurDecomposerWriter logic
            if source_id == "COMMON_INIT":
                file_name = "01_common_init.sql"
            elif source_id == "UNKNOWN_SOURCE":
                file_name = "88_unknown_blocks.sql"
            else:
                file_name = f"10_source_{source_id.lower()}.sql"

            step_file_path = output_root / decomposed.pipeline_id / "processing_steps" / file_name
            
            processing_steps.append({
                "source_id": source_id,
                "file": str(step_file_path.as_posix()),
                "action": "include",  # Default action for cur layer
                "interacted_temp_tables": list(block.interacted_temp_tables)
            })

        # 6. Calculate Delta columns
        logical_keys = primary_key.get('logical_primary_key', [])
        delta_columns = [
            c["name"] for c in columns 
            if c["name"] not in logical_keys and c.get("remark") != "non_original_field"
        ]

        main_file_path = output_root / decomposed.pipeline_id / "processing_steps" / "99_main_dml.sql"
        header_comments_file_path = output_root / decomposed.pipeline_id / "processing_steps" / "00_header_comments.sql"

        pipeline_config = {
            "file_path": str(decomposed.file_path),
            "ddl_source": str(ddl_path.as_posix()),
            "pipeline_id": decomposed.pipeline_id,
            "layer": "cur",  # Hardcode layer as cur
            "model_type": model_type,
            "target_table_name": decomposed.target_table,
            "columns": columns,
            "primary_key": primary_key,
            "partition_key": {
                "logical_partition_key": ['source_key']
            },
            "header_comments": {
                "file": str(header_comments_file_path.as_posix()) if decomposed.header_comments else None
            },
            "main_processing": {
                "file": str(main_file_path.as_posix()) if decomposed.main_sql else None
            },
            "source_processing_steps": processing_steps,
            "temp_table_registry": {k: list(v) for k, v in decomposed.temp_table_registry.items()},
            "delta_columns": delta_columns,
            "applied_source_rule": self.source_rules.get("source", "unknown")
        }
        return pipeline_config

    def write_yaml(self, pipeline_config: dict, output_dir: Path):
        output_dir.mkdir(parents=True, exist_ok=True)
        out_file = output_dir / f"{pipeline_config['pipeline_id']}.yaml"
        with open(out_file, "w", encoding="utf-8") as f:
            yaml.dump(pipeline_config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
