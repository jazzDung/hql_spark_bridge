from pydantic import BaseModel, Field, field_validator, computed_field
from typing import List, Optional, Dict, Any
from datetime import datetime
import re
import os

class ColumnModel(BaseModel):
    """Detailed definition of each data column and cleaning logic"""
    name: str
    data_type: str = Field(alias='type')
    nullable: bool = True
    expression: Optional[str] = None
    is_partition: bool = False
    is_primary_key: bool = False

    def get_select_expression(
        self,
        layer: str,
        trim_space: bool = True,
        null_if_blank_string: bool = False,
        trim_non_breaking_space: bool = False
    ) -> str:
        """Auto-map logic for Raw/Com"""
        if self.expression:
            return f"{self.expression} AS {self.name}"

        is_string_type = any(x in self.data_type.upper() for x in ["VARCHAR", "STRING", "CHAR", "TEXT"])

        # Non-string columns: keep as-is
        if layer not in ["raw", "com"] or not is_string_type:
            return self.name

        expr = self.name

        # Replace NBSP first if requested
        if trim_non_breaking_space:
            expr = f"regexp_replace({expr}, '\\u00A0', ' ')"

        # Trim normal spaces if requested
        if trim_space:
            expr = f"TRIM({expr})"

        # Replace blank string with NULL if requested
        if null_if_blank_string:
            expr = f"NULLIF({expr}, '')"

        return f"{expr} AS {self.name}"

    def get_expression(self, alias: str = None):
        """
        Finds placeholders like {{ field_name }} in the expression and replaces them
        with alias.field_name.
        """
        def replacer(match):
            field_name = match.group(1)
            if alias is None:
                return field_name
            return f"{alias}.{field_name}"

        if self.expression is None:
            return f"{alias}.{self.name}"

        # Regex to find {{ field_name }}
        # It captures 'field_name' in group 1
        return re.sub(r"\{\{\s*(\w+)\s*\}\}", replacer, self.expression)

class SourceModel(BaseModel):
    """Data source information extracted from YAML"""
    alias: str
    source_type: str
    connection_id: str
    query_path: Optional[str] = None
    schema_path: Optional[str] = None
    priority: int = 1


class TargetModel(BaseModel):
    """Target table structure on Datalake or AILab"""
    schema_name: str
    table_name: str
    transformation_model: str = "1"
    partition_keys: List[str] = Field(default_factory=list)
    primary_keys: List[str] = Field(default_factory=list)
    date_keys: List[str] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    """Central metadata orchestrator object for Code Generator"""
    pipeline_id: str
    layer: str
    model_type: str
    sources: List[SourceModel]
    target: TargetModel
    columns: List[ColumnModel] = Field(default_factory=list)
    options: Dict[str, Any] = Field(default_factory=dict)

    # Additional metadata for template
    author: str = Field(default_factory=lambda: os.getlogin())
    created_date: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @field_validator('layer')
    @classmethod
    def validate_layer(cls, v: str) -> str:
        allowed = ['raw', 'com_r', 'com_t', 'com_m', 'cur', 'unl']
        if v.lower() not in allowed:
            raise ValueError(f"Invalid layer '{v}'. Must be one of {allowed}")
        return v.lower()

    def to_dict(self) -> Dict[str, Any]:
        """
        FIX: Do not use model_dump() as it will convert sub-objects into dicts.
        We use __dict__.copy() to preserve Instance Objects.
        """
        # Get outer layer data (shallow copy)
        # At this point self.columns is still List[ColumnModel] (real Objects)
        data = self.__dict__.copy()

        # Pydantic v2 does not automatically put @computed_field into __dict__
        # so we have to load them manually for Jinja to use directly
        data['full_target_name'] = self.full_target_name
        data['template_path_prefix'] = self.template_path_prefix

        return data

    @computed_field
    @property
    def full_target_name(self) -> str:
        """Automatically compute full table name"""
        return f"{self.target.schema_name}.{self.target.table_name}"

    def get_all_columns_select(self) -> List[str]:
        """Aggregate SELECT logic for DML"""
        return [col.get_select_expression(self.layer) for col in self.columns]

    @computed_field
    def template_path_prefix(self) -> str:
        """Automatically build folder prefix based on model: datalake_model_5b"""
        model_id = self.target.transformation_model.lower().replace(" ", "")
        return f"datalake_model_{model_id}"

    def get_template_name(self, file_type: str) -> str:
        """
        Build complete template path.
        file_type: 'hiveql_ddl' or 'hiveql_dml'
        """
        return f"{self.template_path_prefix}/{file_type}/{self.layer}.jinja"
