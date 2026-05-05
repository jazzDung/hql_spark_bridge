import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from typing import Union, Dict, Any

from src.context.sql_conversion_context import JinjaRenderModel
from src.core.type_resolver import TypeResolver
# Import filters module
import src.jinja.filters as filters
# Import the centralized paths
from src.paths import TEMPLATES_DIR


def create_jinja_env(template_dir: Union[str, Path] = TEMPLATES_DIR) -> Environment:
    """
    Creates and configures a Jinja2 environment.

    By default, it uses the centrally defined TEMPLATES_DIR. This ensures that
    templates are found consistently, regardless of where the script is executed.

    :param template_dir: The directory containing the Jinja templates.
                         Defaults to the path defined in src.paths.
    :return: A configured Jinja2 Environment instance.
    """
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        # StrictUndefined raises an error if a variable is missing in the template context.
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # Register custom functions or classes to be available globally in templates.
    env.globals["TypeResolver"] = TypeResolver

    # Register custom filters from filters.py
    # Note: Using update() on env.filters allows bulk registration of functions
    custom_filters = {name: func for name, func in vars(filters).items() if callable(func) and not name.startswith("_")}
    env.filters.update(custom_filters)

    return env

def render_template(template_name: str, render_model: Union[JinjaRenderModel, Dict[str, Any]], template_dir: Union[str, Path] = TEMPLATES_DIR) -> str:
    """
    Renders a Jinja template with the given context data.

    This function is flexible and accepts either a JinjaRenderModel object or a
    standard dictionary for the template context. It uses the centralized
    template directory by default.

    :param template_name: The name of the template file (e.g., 'pyspark/pyspark_basic.jinja').
    :param render_model: An object or dictionary containing data for the template.
    :param template_dir: The root directory for templates. Defaults to the path from src.paths.
    :return: The rendered template as a string.
    """
    # Create the Jinja environment using the specified template directory.
    # The create_jinja_env function now correctly defaults to the central TEMPLATES_DIR.
    env = create_jinja_env(template_dir=template_dir)
    template = env.get_template(template_name)

    # Convert the render_model to a dictionary if it's a JinjaRenderModel instance.
    # This provides the context data for the template.
    context_data = render_model.to_dict() if isinstance(render_model, JinjaRenderModel) else render_model

    return template.render(**context_data)
