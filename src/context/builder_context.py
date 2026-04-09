from pydantic import BaseModel, Field
from typing import List, Optional

class TableTarget(BaseModel):
    schema_name: str
    table_name: str
    partition_keys: List[str] = Field(default_factory=list)

class ColumnMetadata(BaseModel):
    name: str
    data_type: str
    nullable: bool
    is_transformed: bool = False
    expression: Optional[str] = None
    comment: str = 'None'

class PipelineMetadata(BaseModel):
    pipeline_id: str
    layer: str  # raw, com, cur
    source_type: str
    target: TableTarget
    columns: List[ColumnMetadata]

    # Hàm phụ để tự build full name
    @property
    def full_target_name(self) -> str:
        return f"{self.target.schema_name}.{self.target.table_name}"
