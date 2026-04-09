import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from typing import Union, Dict, Any

from src.context.sql_conversion_context import JinjaRenderModel
from src.core.type_resolver import TypeResolver


def create_jinja_env(template_dir: Union[str, Path]) -> Environment:
    template_dir = Path(template_dir)

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        # StrictUndefined raises an error immediately if a variable does not exist in the template
        undefined=StrictUndefined, 
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.globals["TypeResolver"] = TypeResolver
    
    # We removed the wrap_fstring_vars filter because sqlglot already does that at the Transformer layer.
    return env

def render_template(template_name: str, render_model: Union[JinjaRenderModel, Dict[str, Any]], template_dir: str = "template") -> str:
    """
    Flexible render function that accepts a JinjaRenderModel object or a standard dictionary.
    
    :param template_name: Template file name (e.g., 'pyspark/pyspark_basic.jinja')
    :param render_model: Model containing data to fill into the template
    :param template_dir: Path to the root directory containing jinja files.
    :return: The fully rendered string.
    """
    # Get the project root path (assuming the script runs from the root)
    # Using current working directory for flexibility
    root_path = Path(os.getcwd())
    target_template_dir = root_path / template_dir
    
    # If not running from root (e.g., in a notebook), try using an absolute path
    if not target_template_dir.exists():
        target_template_dir = Path(__file__).resolve().parent.parent.parent / template_dir

    env = create_jinja_env(template_dir=target_template_dir)
    template = env.get_template(template_name)
    
    # Get dictionary to pass into jinja
    context_data = render_model.to_dict() if isinstance(render_model, JinjaRenderModel) else render_model
    
    return template.render(**context_data)
