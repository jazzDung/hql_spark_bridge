import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

from src.jinja.environment import render_template
from src.models.pipeline_models import PipelineConfig


class HiveGenerator:
    """
    Module generating HiveQL code (DDL/DML) for Raw, Com, Cur layers.
    Leverages Pydantic models to ensure Metadata consistency.
    """

    def __init__(self, template_root: str = 'templates/'):
        self.template_dir = template_root
        if not os.path.exists(self.template_dir):
            os.makedirs(self.template_dir)
        self.env = Environment(loader=FileSystemLoader(self.template_dir))


    def generate_bundle(self, config: PipelineConfig, output_dir: str):
        """
        Generates full DDL and DML bundle for the Raw layer.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Prepare data context from Pydantic Model
        context = config.to_dict()
        context['created_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Add instance method to context because model_dump() only gets raw data
        # However, Jinja can access object methods if we pass the object in
        context['config_obj'] = config

        template_model_folder = os.path.join("template", f"datalake_model_{config.target.transformation_model.lower()}")
        ddl_template_folder = os.path.join(template_model_folder, "hiveql_ddl")
        dml_template_folder = os.path.join(template_model_folder, "hiveql_dml")

        # 1. Render DDL
        ddl_sql = render_template(f'{config.layer}.jinja', context, ddl_template_folder)

        # 2. Render DML
        dml_sql = render_template(f'{config.layer}.jinja', context, dml_template_folder)

        # 3. Export files
        ddl_file = os.path.join(output_dir, f"ddl_{config.pipeline_id}.sql")
        dml_file = os.path.join(output_dir, f"dml_{config.pipeline_id}.sql")

        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write(ddl_sql)
        with open(dml_file, 'w', encoding='utf-8') as f:
            f.write(dml_sql)

        return ddl_file, dml_file

# Example usage:
# generator = HiveGenerator()
# ddl, dml = generator.generate_raw_bundle(hydrated_config)
