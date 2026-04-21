import yaml
import json
import os
from typing import Dict, List, Any
from src.models.pipeline_models import PipelineConfig, ColumnModel


class YamlHydrator:
    def __init__(self, pipeline_folder: str, schema_folder: str):
        self.pipeline_folder = pipeline_folder
        self.schema_folder = schema_folder

    def load_and_hydrate(self, pipeline_id: str) -> List[PipelineConfig]:
        """Read YAML, merge with JSON Schema and return PipelineConfig object"""

        # 1. Read user's YAML file
        yaml_path = os.path.join(self.pipeline_folder, f"{pipeline_id}.yaml")
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_raw = yaml.safe_load(f)

        # 2. Determine corresponding JSON schema file (from the first source)
        # Assuming Phase 1 has generated the file: metadata/discovery_output/schemas/{table_name}.json
        primary_source = yaml_raw['sources'][0]
        schema_file_name = primary_source.get('schema_path') or f"{primary_source['alias']}.json"
        schema_path = os.path.join(self.schema_folder, schema_file_name)

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Warning: Missing JSON Schema file at {schema_path}. Please run Phase 1 first!")

        with open(schema_path, 'r', encoding='utf-8') as f:
            json_schema = json.load(f)

        pipelines = []

        # 2. Duyệt qua từng target để tạo PipelineConfig riêng
        for t_config in yaml_raw['target']:
            layer = t_config['table_type']

            # Tạo bản copy dữ liệu để build PipelineConfig
            pipeline_data = {
                "pipeline_id": f"{t_config['schema_name']}_{t_config['table_name']}",
                "layer": layer,
                "table_name": t_config['table_name'],
                "model_type": t_config.get('transformation_model', 'overwrite'),
                "sources": yaml_raw['sources'],
                "target": t_config,
                "columns": self._merge_columns(t_config.get('transform_logic', []), json_schema),
                "partition_keys": t_config.get('partition_keys', []),
                "date_keys": t_config.get('date_keys', []),
            }

            # Ép vào Pydantic để validate riêng từng thằng
            pipelines.append(PipelineConfig.model_validate(pipeline_data))




        # # 3. Merge Logic: Auto-hydration
        # hydrated_columns = self._merge_columns(yaml_raw.get('columns', []), json_schema)
        #
        # # Update the column list into the raw data before validating
        # yaml_raw['columns'] = hydrated_columns

        # 4. Type cast and Validate via Pydantic
        # return PipelineConfig.model_validate(yaml_raw)

        return pipelines

    def _merge_columns(self, yaml_columns: List[Dict], json_schema: List[Dict]) -> List[Dict]:
        """Merge columns from JSON (base) and YAML (override)"""
        yaml_col_map = {col['name']: col for col in yaml_columns}
        final_columns = []

        for base_col in json_schema:
            col_name = base_col['name']

            if col_name in yaml_col_map:
                # If present in YAML: Take info from JSON as base, override with YAML
                merged_col = {**base_col, **yaml_col_map[col_name]}
            else:
                # If not present in YAML: Take exactly from JSON
                merged_col = base_col

            final_columns.append(merged_col)

        return final_columns
