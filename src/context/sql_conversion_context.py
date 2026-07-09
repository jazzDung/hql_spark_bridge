from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import sqlglot.expressions as exp

from context.ext_context import ExtContext


@dataclass
class SqlConversionContext:
    """
    Contains the entire ORIGINAL context of a HiveQL file.
    This object is intended to be READ-ONLY for Transformers.
    """
    # Basic file information
    original_file_path: str
    raw_sql_content: str

    # Extracted metadata (e.g., from filename or LOCATION)
    source_name: str
    table_name: str
    layer:  str
    sub_layer: str

    # Stores header comments (file info like Purpose, Author...)
    header_comments: str = ""
    
    # AST (Abstract Syntax Tree) parsed by sqlglot
    ast_nodes: List[exp.Expression] = field(default_factory=list)
    
    # Safe container for specific configurations loaded from YAML (if needed)
    config_rules: Dict[str, Any] = field(default_factory=dict)

    # Additional ext config if it's raw file
    ext_context: ExtContext = None

@dataclass
class JinjaRenderModel:
    """
    Contains the PROCESSED output data, ready to be passed to Jinja for rendering.
    Different script types (Basic, Optimized) may have different fields,
    but the Jinja template will call these specific fields.
    """
    source_name: str
    table_name: str
    layer: str
    
    # List of queries translated into f-string format
    # Example: ['DROP TABLE IF EXISTS {params["raw_schema"]}.my_table', 'CREATE TABLE...']
    transformed_queries: List[str] = field(default_factory=list)
    
    # Additional variables for the Template
    is_partitioned: bool = False
    
    # Pass header comments to Jinja to print at the top of the Python file
    header_comments: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts to dict to pass as **kwargs into jinja_template.render()"""
        return self.__dict__
