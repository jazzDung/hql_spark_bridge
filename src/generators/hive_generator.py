import os
from datetime import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Import the centralized paths and the refactored render_template function
from src.jinja.environment import render_template
from src.models.pipeline_models import PipelineConfig
from src.paths import TEMPLATES_DIR


class HiveGenerator:
    """
    Generates HiveQL code (DDL/DML) for different data layers.
    This class uses Pydantic models to ensure metadata consistency and leverages
    a centralized Jinja2 environment for template rendering.
    """

    def __init__(self, template_root: Path = TEMPLATES_DIR):
        """
        Initializes the generator with the root directory for templates.

        :param template_root: The root path for Jinja templates. Defaults to the
                              centrally defined TEMPLATES_DIR.
        """
        # The template directory is now managed by the centralized paths module.
        # There's no need to create it here; we just reference it.
        self.template_dir = template_root
        # The environment creation is handled by the render_template function,
        # so we don't need a dedicated Environment object in the class itself.

    def generate_bundle(self, config: PipelineConfig, output_dir: str):
        """
        Generates a full DDL and DML bundle for a given pipeline configuration.

        :param config: The pipeline configuration object.
        :param output_dir: The directory where the generated SQL files will be saved.
        """
        # Ensure the output directory exists.
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Prepare the context data for the Jinja templates.
        context = config.to_dict()
        context['created_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Pass the original config object to access methods if needed in the template.
        context['config_obj'] = config

        # Construct paths to template subdirectories using pathlib for robustness.
        template_model_folder = self.template_dir / f"datalake_model_{config.target.transformation_model.lower()}"
        ddl_template_folder = template_model_folder / "hiveql_ddl"
        dml_template_folder = template_model_folder / "hiveql_dml"

        # 1. Render the DDL template.
        ddl_sql = render_template(f'{config.layer}.jinja', context, template_dir=ddl_template_folder)

        # 2. Render the DML template.
        dml_sql = render_template(f'{config.layer}.jinja', context, template_dir=dml_template_folder)

        # 3. Write the rendered SQL to output files.
        ddl_file = output_path / f"ddl_{config.pipeline_id}.sql"
        dml_file = output_path / f"dml_{config.pipeline_id}.sql"

        ddl_file.write_text(ddl_sql, encoding='utf-8')
        dml_file.write_text(dml_sql, encoding='utf-8')

        return str(ddl_file), str(dml_file)
