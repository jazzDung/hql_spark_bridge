import os
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from src.jinja.environment import render_template
from src.migration.ddl_enricher import DdlEnricher
from src.paths import PROJECT_ROOT

class PySparkGenerator:
    def __init__(self, source_rules: Dict[str, Any], output_mode: str = "simple"):
        self.output_mode = output_mode
        self.source_rules = source_rules
        self.template_dir = PROJECT_ROOT / "template" / "migration"

    def generate(self, pipeline_config: Dict[str, Any], output_root: Path):
        # 1. Generate DDL
        self._generate_ddl(pipeline_config, output_root)

        # 2. Generate DML
        if self.output_mode == "simple":
            self._generate_simple_dml(pipeline_config, output_root)
        else:
            self._generate_complex_dml(pipeline_config, output_root)

    def _generate_ddl(self, pipeline_config: Dict[str, Any], output_root: Path):
        enricher = DdlEnricher(source_rules=self.source_rules)
        ddl_context = enricher.enrich(pipeline_config)
        ddl_context["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        model_type = pipeline_config.get("model_type", "3")
        template_name = f"model_{model_type}/com_t_ddl.jinja"
        
        try:
            # Tái sử dụng hàm render_template từ src.jinja.environment
            ddl_sql = render_template(template_name, ddl_context, template_dir=self.template_dir)
            
            ddl_dir = output_root / "ddl"
            ddl_dir.mkdir(parents=True, exist_ok=True)
            ddl_file = ddl_dir / f"{pipeline_config['layer']}_{pipeline_config['target_table_name']}.sql"
            ddl_file.write_text(ddl_sql, encoding="utf-8")

            general_ddl_file = PROJECT_ROOT / "output" / "migration"/ "ddl" /  pipeline_config["layer"]  / f"{pipeline_config['layer']}_{pipeline_config['target_table_name']}.sql"
            general_ddl_file.write_text(ddl_sql, encoding="utf-8")
            print(f"Generated DDL at {ddl_file}")
        except Exception as e:
            print(f"Error rendering DDL template {template_name}: {e}")

    def _generate_simple_dml(self, pipeline_config: Dict[str, Any], output_root: Path):
        model_type = pipeline_config.get("model_type", "3")
        template_name = f"model_{model_type}/com_t_dml.jinja"
        
        original_columns = [
            col for col in pipeline_config.get("columns", [])
            if col.get("remark") != "non_original_field"
        ]
        
        # Read pre_processing SQLs
        pre_processing_sqls = []
        for step in pipeline_config.get("pre_processing", []):
            if step.get("action") == "skip":
                continue
            step_file = Path(step["file"])
            if step_file.exists():
                pre_processing_sqls.append(step_file.read_text(encoding="utf-8"))

        # Extract base_table from target_table_name
        # e.g., t_k2_cif_alias -> cif_alias
        target_table_name = pipeline_config["target_table_name"]
        source_name = pipeline_config["source_name"]
        base_table = target_table_name.replace(f"t_{source_name}_", "")


        dml_context = {
            "pipeline_id": pipeline_config["pipeline_id"],
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "model_type": model_type,
            "target_table_name": target_table_name,
            "source_name": source_name,
            "base_table": base_table,
            "original_columns": original_columns,
            "delta_columns": pipeline_config.get("delta_columns", []),
            "key": pipeline_config["key"],
            "pre_processing_sqls": pre_processing_sqls
        }

        try:
            # Tái sử dụng hàm render_template từ src.jinja.environment
            dml_code = render_template(template_name, dml_context, template_dir=self.template_dir)
            
            dml_dir = output_root / "dml"
            dml_dir.mkdir(parents=True, exist_ok=True)
            dml_file = dml_dir / f"{pipeline_config['layer']}_{pipeline_config['target_table_name']}.py"
            dml_file.write_text(dml_code, encoding="utf-8")

            general_dml_file = PROJECT_ROOT / "output" / "migration"/ "dml" /  pipeline_config["layer"]  / f"{pipeline_config['layer']}_{pipeline_config['target_table_name']}.py"
            general_dml_file.write_text(dml_code, encoding="utf-8")

            print(f"Generated DML at {dml_file}")
        except Exception as e:
            print(f"Error rendering DML template {template_name}: {e}")

    def _generate_complex_dml(self, pipeline_config: Dict[str, Any], output_root: Path):
        print("Complex mode is a placeholder and has not been implemented yet.")
