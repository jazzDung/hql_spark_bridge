from abc import ABC, abstractmethod
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel

class BaseSqlTransformer(ABC):
    """
    Core template for all transformation branches.
    Only 1 Input and 1 Output. Does not interfere with system state.
    """
    
    @abstractmethod
    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        """
        Receives SQL context (with extracted AST).
        Returns data model for Jinja to render into file.
        """
        pass
