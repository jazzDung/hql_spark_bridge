import os
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import sqlglot

from src.core.parser import HiveScriptParser
from src.jinja.environment import render_template
from src.migration.ddl_resolver import DdlResolver
from src.paths import PROJECT_ROOT
from src.context.sql_conversion_context import SqlConversionContext
from src.transformers.com_pyspark_transformer import ComPySparkTransformer
from src.transformers.raw_pyspark_transformer import RawPySparkTransformer
from transformers.utils import format_header_comments


class ComPySparkGenerator:
    def __init__(self, source_rules: Dict[str, Any], output_mode: str = "simple"):
        self.output_mode = output_mode
        self.source_rules = source_rules
        self.template_dir = PROJECT_ROOT / "template" / "migration"
        self.transformer = ComPySparkTransformer()
        self.raw_transformer = RawPySparkTransformer()

    def generate(self, pipeline_config: Dict[str, Any], output_root: Path):
        # 1. Generate DDL
        ddl_context = self._generate_ddl(pipeline_config, output_root)

        # 2. Generate DML
        if self.output_mode == "simple":
            dml_context = self._generate_simple_dml(pipeline_config, output_root)
        else:
            dml_context = self._generate_complex_dml(pipeline_config, output_root)
            
        # 3. Generate Basic Conversion
        self._generate_basic_conversion(pipeline_config, output_root)

        return ddl_context, dml_context

    def _generate_ddl(self, pipeline_config: Dict[str, Any], output_root: Path):
        layer = pipeline_config.get("layer")
        if not layer:
            print("Error: 'layer' not found in pipeline_config.")
            return None

        model_folders = [d for d in self.template_dir.iterdir() if d.is_dir() and d.name.startswith('model_')]

        for model_folder in model_folders:
            model_name = model_folder.name
            enricher = DdlResolver(source_rules=self.source_rules)
            ddl_context = enricher.enrich(pipeline_config, model_type=model_name.split("_")[-1].lower())
            ddl_context["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ddl_context["partition_columns"] = ["<<partition_column>>"]

            # Find templates containing the layer name and "ddl"
            # e.g., com_t_ddl.jinja for layer 'com'
            ddl_templates = [p for p in model_folder.glob("*ddl.jinja") if f"_{layer}_" in p.name or p.name.startswith(f"{layer}_")]

            for template_path in ddl_templates:
                template_name = f"{model_name}/{template_path.name}"
                try:
                    ddl_sql = render_template(template_name, ddl_context, template_dir=self.template_dir)
                    
                    output_dir = output_root / model_name / "ddl"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Use a descriptive name for the output file
                    output_filename = f"{pipeline_config['layer'].lower()}_{pipeline_config['target_table_name'].lower()}_{template_path.stem}.sql"
                    ddl_file = output_dir / output_filename
                    
                    ddl_file.write_text(ddl_sql, encoding="utf-8")
                    # print(f"Generated DDL from '{template_name}' at {ddl_file}")

                except Exception as e:
                    print(f"Error rendering DDL template {template_name}: {e}")

        return ddl_context

    def _generate_simple_dml(self, pipeline_config: Dict[str, Any], output_root: Path):
        original_columns = [
            col for col in pipeline_config.get("columns", [])
            if col.get("remark") != "non_original_field"
        ]

        # --- Process Header Comments ---
        try:
            header_comments_path = PROJECT_ROOT / Path(pipeline_config.get("header_comments").get("file"))
            header_comments = header_comments_path.read_text(encoding="utf-8")
            formatted_header_comments = format_header_comments(header_comments)
        except:
            formatted_header_comments = ""
        
        # Extract base_table from target_table_name
        target_table_name = pipeline_config["target_table_name"]
        source_name = pipeline_config["source_name"]
        base_table = target_table_name.replace(f"t_{source_name}_", "")

        # Read pre_processing SQLs
        pre_processing_sqls = []


        # for step in pipeline_config.get("pre_processing", []):
        #     if step.get("action") == "skip":
        #         continue
        #     step_file = PROJECT_ROOT / Path(step["file"])
        #     # print(f"Reading pre-processing SQL from: {step_file.stem}")
        #
        #     if step_file.exists():
        #         # Create a context for the transformer
        #         context = HiveScriptParser.parse_file(str(step_file))
        #         # print(context.source_name, context.sub_layer)
        #
        #         # Apply the ComPysparkTransformer
        #         jinja_render_model = self.transformer.transform(pipeline_config, context)
        #
        #         # Extract the transformed queries
        #         for query_obj in jinja_render_model.transformed_queries:
        #              if isinstance(query_obj, dict) and query_obj.get('type') == 'query':
        #                  pre_processing_sqls.append(query_obj.get('content'))
        #              elif isinstance(query_obj, str): # In case it's just a list of strings
        #                  pre_processing_sqls.append(query_obj)

        # Non main sql
        non_main_processing_sql_path = PROJECT_ROOT / Path(pipeline_config.get("non_main_processing").get("file"))
        context = HiveScriptParser.parse_file(str(non_main_processing_sql_path))
        # print(context.source_name, context.sub_layer)

        # Apply the ComPysparkTransformer
        jinja_render_model = self.transformer.transform(pipeline_config, context)

        # Extract the transformed queries
        for query_obj in jinja_render_model.transformed_queries:
            if isinstance(query_obj, dict) and query_obj.get('type') == 'query':
                pre_processing_sqls.append(query_obj.get('content'))
            elif isinstance(query_obj, str):  # In case it's just a list of strings
                pre_processing_sqls.append(query_obj)


        # Read main processing SQLs
        main_processing_sqls = []
        main_processing_sql_path = PROJECT_ROOT / Path(pipeline_config.get("main_processing").get("file"))
        main_processing_context = HiveScriptParser.parse_file(str(main_processing_sql_path))
        main_processing_render_model = self.transformer.transform(pipeline_config, main_processing_context)

        for query_obj in main_processing_render_model.transformed_queries:
             if isinstance(query_obj, dict) and query_obj.get('type') == 'query':
                 main_processing_sqls.append(query_obj.get('content'))
             elif isinstance(query_obj, str): # In case it's just a list of strings
                 main_processing_sqls.append(query_obj)


        # Primary_key
        primary_key = pipeline_config["primary_key"].get('logical_primary_key', ["<<primary_key>>"])
        if len(primary_key) == 0:
            primary_key = ["<<primary_key>>"]

        dml_context = {
            "pipeline_id": pipeline_config["pipeline_id"],
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "target_table_name": target_table_name,
            "source_name": source_name,
            "base_table": base_table,
            "original_columns": original_columns,
            "header_comments": formatted_header_comments,
            "delta_columns": pipeline_config.get("delta_columns", []),
                "primary_key": primary_key,
            "partition_columns": ["<<partition_column>>"],
            "key_date_column": "<<key_date_column>>",
            "main_processing_sqls": main_processing_sqls,
            "pre_processing_sqls": pre_processing_sqls
        }

        layer = pipeline_config.get("layer")
        if not layer:
            print("Error: 'layer' not found in pipeline_config.")
            return dml_context

        model_folders = [d for d in self.template_dir.iterdir() if d.is_dir() and d.name.startswith('model_')]

        for model_folder in model_folders:
            model_name = model_folder.name
            # Find templates containing the layer name and "dml"
            dml_templates = [p for p in model_folder.glob("*dml*.jinja") if f"_{layer}_" in p.name or p.name.startswith(f"{layer}_")]

            for template_path in dml_templates:
                template_name = f"{model_name}/{template_path.name}"
                try:
                    model_type = model_name.split('_')[-1]
                    dml_context["model_type"] = model_type

                    dml_code = render_template(template_name, dml_context, template_dir=self.template_dir)
                    
                    output_dir = output_root / model_name / "dml"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    output_filename = f"{pipeline_config['layer'].lower()}_{pipeline_config['target_table_name'].lower()}_{template_path.stem}.py"
                    dml_file = output_dir / output_filename
                    
                    dml_file.write_text(dml_code, encoding="utf-8")
                    # print(f"Generated DML from '{template_name}' at {dml_file}")

                except Exception as e:
                    print(f"Error rendering DML template {template_name}: {e}")

        return dml_context

    def _generate_basic_conversion(self, pipeline_config: Dict[str, Any], output_root: Path):
        """Generates basic conversion scripts using RawPySparkTransformer"""
        transformer = RawPySparkTransformer(config_root=PROJECT_ROOT / "configs")
        context = HiveScriptParser.parse_file(pipeline_config["file_path"])
        render_model = transformer.transform(context)

        dml_code = render_template(
            template_name="pyspark/optimized_pyspark.jinja",
            render_model=render_model
        )

        # 2. Load mapping configuration from YAML

        output_dir = output_root / "basic_conversion"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use a descriptive name for the output file
        output_filename = f"{pipeline_config['layer']}_{pipeline_config['target_table_name'].lower()}.sql"
        ddl_file = output_dir / output_filename

        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(dml_code)

    def _generate_complex_dml(self, pipeline_config: Dict[str, Any], output_root: Path):
        print("Complex mode is a placeholder and has not been implemented yet.")