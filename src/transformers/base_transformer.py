from abc import ABC, abstractmethod
from src.context.sql_conversion_context import SqlConversionContext, JinjaRenderModel

class BaseSqlTransformer(ABC):
    """
    Khuôn mẫu cốt lõi cho mọi nhánh biến đổi.
    Chỉ có 1 Input và 1 Output. Không can thiệp vào trạng thái hệ thống.
    """
    
    @abstractmethod
    def transform(self, context: SqlConversionContext) -> JinjaRenderModel:
        """
        Nhận vào context SQL (Đã bóc tách AST).
        Trả ra model dữ liệu để Jinja ném vào file.
        """
        pass
