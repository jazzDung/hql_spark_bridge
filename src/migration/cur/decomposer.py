# src/migration/decomposer.py
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Set
import sqlglot
import sqlglot.expressions as exp


@dataclass
class CurSourceBlock:
    source_id: str
    ast_nodes: list[exp.Expression] = field(default_factory=list)
    # Lưu trữ các tên bảng tạm (Temp Table) mà Source này tương tác
    interacted_temp_tables: Set[str] = field(default_factory=set)

    @property
    def raw_sql(self) -> str:
        return "\n\n".join(f"{stmt.sql(dialect='hive', pretty=True)};" for stmt in self.ast_nodes)


@dataclass
class CurDecomposedScript:
    file_path: Path
    pipeline_id: str
    target_table: str
    temp_table_registry: dict
    source_blocks: Dict[str, CurSourceBlock]
    main_sql: str  # <-- TRƯỜNG MỚI BỔ SUNG
    raw_sql_full: str
    header_comments: str = ""


class CurSqlDecomposer:
    def __init__(self, target_schema: str = "cur", com_schema: str = "com"):
        self.target_schema = target_schema
        self.com_schema = com_schema

    def decompose(self, sql_file: Path) -> CurDecomposedScript:
        content = sql_file.read_text(encoding="utf-8")

        header_lines, sql_lines = [], []
        is_header = True
        for line in content.split('\n'):
            stripped = line.strip()
            if is_header and (stripped.startswith('--') or not stripped):
                header_lines.append(line)
            else:
                is_header = False
                sql_lines.append(line)

        header_comments = '\n'.join(header_lines).strip()
        sql_content = '\n'.join(sql_lines)

        pipeline_id = sql_file.stem
        target_table = pipeline_id.replace("cur_", "").upper()


        # ... (đoạn đọc file và parse giống cũ) ...
        statements = sqlglot.parse(sql_content, read="hive", error_level=sqlglot.ErrorLevel.WARN)

        # Khởi tạo 2 cấu trúc để theo dõi state
        temp_table_registry = defaultdict(set)

        # temp_table_registry: Dict[str, str] = {} # {TEMP_TABLE_NAME: SOURCE_ID}
        source_blocks: Dict[str, CurSourceBlock] = {}
        main_stmts = []
        registry_list = []

        # --- VÒNG 1: Xây dựng Registry (Ai sở hữu bảng nào?) ---
        for stmt in statements:
            source_id, _ = self.analyze_source_id(stmt, target_table, temp_table_registry)
            if source_id not in ["FINAL_CONSOLIDATION", "UNKNOWN_SOURCE", "COMMON_INIT"]:
                # Nếu là lệnh INSERT/CREATE INTO bảng tạm, ghi danh chủ sở hữu
                mutated_table = self._extract_mutated_table_name(stmt)
                if mutated_table and mutated_table.upper().startswith('TEMP_'):
                    temp_table_registry[source_id].add(mutated_table.upper())

        # --- VÒNG 2: Phân phối câu lệnh vào các block ---
        for stmt in statements:
            # 1. Xác định Main SQL trước
            mutated_table = self._extract_mutated_table_name(stmt)
            if mutated_table == target_table:
                main_stmts.append(stmt)
                continue

            def update_source_blocks(sb: Dict[str, CurSourceBlock], sid: str, s, mt: str | None):
                # 3. Gom vào block
                if source_id not in sb:
                    sb[sid] = CurSourceBlock(source_id=sid)

                sb[sid].ast_nodes.append(s)

                if mt is not None:
                    sb[sid].interacted_temp_tables.add(mt.upper())

            # 2. Xác định chủ sở hữu (Source)
            if  isinstance(stmt, (exp.Create, exp.Drop, exp.TruncateTable)):
                source_ids = list(self._strategy_mutation_query(stmt, temp_table_registry))
                for source_id in source_ids:
                    update_source_blocks(source_blocks, source_id, stmt, mutated_table)
            else:
                source_id, _ = self.analyze_source_id(stmt, target_table, temp_table_registry)
                update_source_blocks(source_blocks, source_id, stmt, mutated_table)



        # Convert main sql to text
        main_sql = "\n\n".join(f"{stmt.sql(dialect='hive', pretty=True)};" for stmt in main_stmts)

        return CurDecomposedScript(
            file_path=sql_file,
            pipeline_id=pipeline_id,
            target_table=target_table,
            temp_table_registry=temp_table_registry,
            source_blocks=source_blocks,
            main_sql=main_sql,
            raw_sql_full=content,
            header_comments=header_comments
        )

    # def _determine_source_recursive(self, stmt: exp.Expression, target_table: str, registry: Dict[str, str]) -> str:
    #     """
    #     Dùng logic truy vết: Nếu lệnh này dùng bảng tạm X,
    #     mà X thuộc sở hữu của Source Y -> Lệnh này thuộc Source Y.
    #     """
    #     # Luật trực tiếp (Hardcoded SOURCE_NAME)
    #     direct_source, _, _ = self.analyze_source_id(stmt, target_table)
    #     if direct_source not in ["FINAL_CONSOLIDATION", "UNKNOWN_SOURCE"]:
    #         return direct_source
    #
    #     # Luật truy vết (Lineage Tracking)
    #     tables = list(stmt.find_all(exp.Table))
    #     for t in tables:
    #         table_name = t.name.upper()
    #         if table_name in registry:
    #             return registry[table_name]  # Trả về Source của bảng tạm đó
    #
    #     return "UNKNOWN_SOURCE"

    # def _determine_source_recursive(self,stmt: exp.Expression, target_table: str, registry: Dict[str, str]) -> str:
    #     """
    #     Dùng logic truy vết: Nếu lệnh này dùng bảng tạm X,
    #     mà X thuộc sở hữu của Source Y -> Lệnh này thuộc Source Y.
    #     """
    #     # Luật trực tiếp (Hardcoded SOURCE_NAME)
    #     direct_source, _ = self.analyze_source_id(stmt, target_table, registry)
    #     if direct_source in ["FINAL_CONSOLIDATION", "UNKNOWN_SOURCE"] or direct_source in registry:
    #         return direct_source
    #
    #     # Luật truy vết (Lineage Tracking)
    #     table_name = direct_source.upper()
    #     for source in registry:
    #         if table_name in registry[source]:
    #             return source  # Trả về Source của bảng tạm đó
    #
    #     return "UNKNOWN_SOURCE"


    def _extract_mutated_table_name(self, stmt: exp.Expression) -> Optional[str]:
        """
        Chỉ lấy tên bảng LÀ MỤC TIÊU BỊ BIẾN ĐỔI (Insert, Alter, Drop, Analyze).
        Giúp tránh bẫy bảng đích bị gọi lại trong mệnh đề FROM / JOIN.
        """
        if isinstance(stmt, (exp.Create, exp.Drop, exp.Alter, exp.TruncateTable, exp.Insert)):
            tbl = stmt.find(exp.Table)
            return tbl.name.upper() if tbl else None
        elif isinstance(stmt, exp.Command) and "ANALYZE" in stmt.sql().upper():
            tbl = stmt.find(exp.Table)
            return tbl.name.upper() if tbl else None
        return None


    def _group_and_extract(self, statements: List[exp.Expression], target_table: str) -> tuple[
        Dict[str, CurSourceBlock], Dict, List[exp.Expression]]:
        blocks: Dict[str, CurSourceBlock] = {}
        ddl_registry: Dict[str, List[exp.Expression]] = {}
        main_stmts: List[exp.Expression] = []

        for stmt in statements:
            if not stmt:
                continue

            # LUẬT ƯU TIÊN 1: Bắt Main SQL
            # Nếu mục tiêu biến đổi chính là target_table (VD: DIM_CONTACT)
            mutated_table = self._extract_mutated_table_name(stmt)
            if mutated_table == target_table:
                main_stmts.append(stmt)
                continue  # Bỏ qua, không đưa vào source_blocks nữa

            # Quá trình analyze Source ID cũ
            source_id, temp_table_name = self.analyze_source_id(stmt, target_table, ddl_registry)

            if temp_table_name:
                if temp_table_name not in ddl_registry:
                    ddl_registry[temp_table_name] = []
                ddl_registry[temp_table_name].append(stmt)
                continue

            if source_id not in blocks:
                blocks[source_id] = CurSourceBlock(source_id=source_id)

            blocks[source_id].ast_nodes.append(stmt)

        return blocks, ddl_registry, main_stmts


    def _distribute_ddls_to_sources(self, blocks: Dict[str, CurSourceBlock],
                                    ddl_registry: Dict[str, List[exp.Expression]]):
        """
        Nhúng các lệnh DDL (Drop/Create) lên đầu mỗi Source Block
        nếu Source đó có thực hiện INSERT vào bảng tạm tương ứng.
        """
        for source_id, block in blocks.items():
            if source_id in ["FINAL_CONSOLIDATION", "UNKNOWN_SOURCE"]:
                continue

            ddls_to_prepend = []
            for temp_table in block.interacted_temp_tables:
                if temp_table in ddl_registry:
                    # Lấy bản copy của lệnh DDL để nhúng vào block
                    for ddl_stmt in ddl_registry[temp_table]:
                        ddls_to_prepend.append(ddl_stmt.copy())

            # Chèn các lệnh DDL lên đầu mảng ast_nodes của block hiện tại
            block.ast_nodes = ddls_to_prepend + block.ast_nodes

    def get_source_tables(self, stmt: exp.Expression) -> list[str]:
        """
        Trích xuất tất cả các bảng nguồn thực sự từ một câu lệnh SQL.
        Bỏ qua bảng đích (INSERT/CREATE) và bỏ qua các CTE.
        """
        # 1. Thu thập tên các CTE để loại trừ (ví dụ: temp_dim_account_contact_m21_row_num)
        cte_names = {cte.alias.upper() for cte in stmt.find_all(exp.CTE)}

        # 2. Thu thập "Node ID" của các bảng đích (INSERT INTO, CREATE TABLE)
        # Dùng id() để chỉ loại bỏ đúng cái node đó, không loại bỏ nhầm nếu bảng bị gọi lại
        mutated_nodes = set()
        for node in stmt.find_all((exp.Insert, exp.Create, exp.Drop, exp.TruncateTable)):
            if isinstance(node.this, exp.Schema):
                mutated_nodes.add(id(node.this.this))
            if isinstance(node.this, exp.Table):
                mutated_nodes.add(id(node.this))

        source_tables = set()

        # 3. Quét mọi node Table trong toàn bộ cây AST (bao gồm cả trong CTE, UNION)
        for table in stmt.find_all(exp.Table):
            # Bỏ qua nếu node này chính là bảng đích đang bị INSERT/CREATE
            if id(table) in mutated_nodes:
                continue

            name = table.name.upper()

            # Bỏ qua nếu bảng đang được gọi thực chất chỉ là một CTE (bảng ảo)
            if name in cte_names:
                continue

            source_tables.add(name)

        return list(source_tables)

    def _strategy_partition_key(self, stmt: exp.Expression) -> Optional[str]:
        """
        Chiến thuật 1 (Mạnh nhất): Tìm khai báo PARTITION(SOURCE_KEY = 'XYZ')
        """
        for partition in stmt.find_all(exp.Partition):
            for eq in partition.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and eq.left.name.upper() in ('SOURCE_KEY', 'SOURCE_NAME'):
                    if isinstance(eq.right, exp.Literal):
                        return eq.right.name.upper()
        return None

    def _strategy_source_key_assignment(self, stmt: exp.Expression) -> Optional[str]:
        """
        Chiến thuật 2: Tìm khai báo 'XYZ' AS SOURCE_NAME.
        Sqlglot find_all() tự động đệ quy xuyên qua CTEs, UNIONs, và Subqueries.
        """
        detected_sources = set()

        # Quét toàn bộ AST để tìm mọi Alias
        for alias in stmt.find_all(exp.Alias):
            if alias.alias.upper() == 'SOURCE_KEY':
                if isinstance(alias.this, exp.Literal):
                    detected_sources.add(alias.this.name.upper())

        if len(detected_sources) == 1:
            return list(detected_sources)[0]
        elif len(detected_sources) > 1:
            # Nếu UNION 2 nguồn khác nhau (MHBOS và GUAVA), đây là bước gom data
            return "FINAL_CONSOLIDATION"

        return None

    def _strategy_source_name_assignment(self, stmt: exp.Expression) -> Optional[str]:
        """
        Chiến thuật 2: Tìm khai báo 'XYZ' AS SOURCE_NAME.
        Sqlglot find_all() tự động đệ quy xuyên qua CTEs, UNIONs, và Subqueries.
        """
        detected_sources = set()

        # Quét toàn bộ AST để tìm mọi Alias
        for alias in stmt.find_all(exp.Alias):
            if alias.alias.upper() == 'SOURCE_NAME':
                if isinstance(alias.this, exp.Literal):
                    detected_sources.add(alias.this.name.upper())

        if len(detected_sources) == 1:
            return list(detected_sources)[0]
        elif len(detected_sources) > 1:
            # Nếu UNION 2 nguồn khác nhau (MHBOS và GUAVA), đây là bước gom data
            return "FINAL_CONSOLIDATION"

        return None

    def _strategy_registry_fallback(self, stmt: exp.Expression, registry: Dict[str, Set[str]]) -> Optional[str]:
        """
        Chiến thuật "Desperate Attempt":
        Đối chiếu các bảng nguồn thực sự của câu lệnh với Registry tự điển.
        """
        # Bước 1: Lấy danh sách bảng nguồn thực sự (bỏ qua CTE, bỏ qua Target Table)
        source_tables: List[str] = self.get_source_tables(stmt)

        if not source_tables:
            return None

        matched_sources = set()

        # Bước 2: Quét đối chiếu với Registry
        for table in source_tables:
            table_name_upper = table.upper()

            for source_name, dependent_tables in registry.items():
                # Đảm bảo các bảng trong registry cũng được viết hoa để so sánh chuẩn
                dependent_tables_upper = {t.upper() for t in dependent_tables}

                if table_name_upper in dependent_tables_upper:
                    matched_sources.add(source_name)

        # Bước 3: Đánh giá kết quả
        if len(matched_sources) == 1:
            return list(matched_sources)[0]
        elif len(matched_sources) > 1:
            # Lệnh này dùng các bảng thuộc nhiều nguồn khác nhau -> Đây là bước gom (Consolidation)
            return "FINAL_CONSOLIDATION"

        return None

    def _strategy_table_prefix(self, stmt: exp.Expression, target_table: str) -> Optional[str]:
        """
        Chiến thuật 3 (Fallback): Bắt nguồn dựa trên tiền tố của bảng trong FROM/JOIN.
        Ví dụ: T_RAK_CUSTOMER -> RAK
        """
        source_candidates = set()

        for t in stmt.find_all(exp.Table):
            name = t.name.upper()
            # Bỏ qua bảng đích nếu nó bị gọi lại trong FROM
            if name == target_table:
                continue

            parts = name.split('_')
            if len(parts) >= 2 and parts[0] in ('T', 'M', 'R'):
                source_candidates.add(parts[1])

        if len(source_candidates) == 1:
            return list(source_candidates)[0]
        elif len(source_candidates) > 1:
            return "FINAL_CONSOLIDATION"

        return None

    def _strategy_mutation_query(self, stmt: exp.Expression, registry: Dict[str, Set[str]]) -> set[str]:

        matched_sources = set()

        for t in stmt.find_all(exp.Table):
            table = t.name
            table_name_upper = table.upper()

            for source_name, dependent_tables in registry.items():
                # Đảm bảo các bảng trong registry cũng được viết hoa để so sánh chuẩn
                dependent_tables_upper = {t.upper() for t in dependent_tables}

                if table_name_upper in dependent_tables_upper:
                    matched_sources.add(source_name)

        # Bước 3: Đánh giá kết quả
        return matched_sources


    def analyze_source_id(self, stmt: exp.Expression, target_table: str, registry: dict) -> tuple[str, str]:
        """
        Hàm tổng điều phối. Chạy tuần tự các chiến thuật theo độ ưu tiên.
        """
        # Bước 1: Lọc DDL khởi tạo (COMMON INIT)
        mutated_table = self._extract_mutated_table_name(stmt)

        # if isinstance(stmt, (exp.Create, exp.Drop, exp.TruncateTable)):
        #     for t in stmt.find_all(exp.Table):
        #         if t.name.upper().startswith('TEMP_'):
        #             return "COMMON_INIT", mutated_table

        # Bước 2: Danh sách chiến thuật và Tham số (Lazy Evaluation)
        # Định nghĩa bằng lambda để chỉ chạy khi chiến thuật trước đó thất bại (tiết kiệm Performance)
        strategies = {
            "PARTITION_KEY": lambda: self._strategy_partition_key(stmt),
            "SOURCE_KEY_ASSIGNMENT": lambda: self._strategy_source_key_assignment(stmt),
            "SOURCE_NAME_ASSIGNMENT": lambda: self._strategy_source_name_assignment(stmt),
            "TABLE_PREFIX": lambda: self._strategy_table_prefix(stmt, target_table),
            "REGISTRY": lambda: self._strategy_registry_fallback(stmt, registry),
            # "MUTATION_QUERY": lambda: self._strategy_mutation_query(stmt, registry),
        }

        # Bước 3: Đánh giá theo Priority
        for strategy_name, strategy_func in strategies.items():
            result = strategy_func()
            if result is not None:
                # Bạn có thể bật logging ở đây để xem script nào được bóc bởi chiến thuật nào
                # print(f"Matched {result} via {strategy_name}")
                return result, mutated_table

        # Nếu tất cả các chiến thuật đều thất bại
        return "UNKNOWN_SOURCE", mutated_table


class CurDecomposerWriter:
    def write(self, decomposed: CurDecomposedScript, output_root: Path):
        steps_dir = output_root / "processing_steps"
        steps_dir.mkdir(parents=True, exist_ok=True)

        if decomposed.header_comments:
            header_file = steps_dir / "00_header_comments.sql"
            header_file.write_text(decomposed.header_comments, encoding="utf-8")

        for source_id, block in decomposed.source_blocks.items():
            if source_id == "COMMON_INIT":
                file_name = "01_common_init.sql"
            elif source_id == "UNKNOWN_SOURCE":
                file_name = "88_unknown_blocks.sql"
            else:
                file_name = f"10_source_{source_id.lower()}.sql"

            out_file = steps_dir / file_name
            out_file.write_text(block.raw_sql, encoding="utf-8")

        # GHI MAIN SQL RA FILE ĐỘC LẬP
        if decomposed.main_sql:
            main_file = steps_dir / "99_main_dml.sql"
            main_file.write_text(decomposed.main_sql, encoding="utf-8")