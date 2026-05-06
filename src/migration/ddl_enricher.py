from typing import Dict, Any

from src.utils.source_rule_loader import load_all_layer_extra_fields


class DdlEnricher:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules
        # Global extra fields for models
        self.model_extra_fields = load_all_layer_extra_fields()

    def enrich(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        model_type = str(pipeline_config.get("model_type", "3")).lower()
        
        # Lấy extra fields từ model mapping, ghi đè nếu source_rules có quy định riêng
        model_layer: str = str(pipeline_config.get('layer'))
        extra_fields = self.model_extra_fields.get(model_layer).get(model_type, [])
        # source_extra_fields = self.source_rules.get("extra_ddl_fields", {}).get(model_type)
        # if source_extra_fields:
        #     extra_fields = source_extra_fields
        # print(model_layer, model_type)
        # print(self.model_extra_fields)

        # Lọc bỏ các cột được đánh dấu là non_original_field
        filtered_columns = [
            col for col in pipeline_config.get("columns", [])
            if col.get("remark") != "non_original_field"
        ]

        return {
            "columns": filtered_columns,
            "extra_fields": extra_fields,
            "model_type": model_type,
            "target_table_name": pipeline_config["target_table_name"],
            "pipeline_id": pipeline_config["pipeline_id"],
            "com_schema": "${com_schema}"
        }
