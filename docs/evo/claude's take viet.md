# HQL Spark Bridge — Chế độ C: Di cư (Migration) Legacy SQL
## Master Technical Specification & Implementation Guide (Bản Đặc tả Kỹ thuật & Hướng dẫn Triển khai)

> **Phạm vi (Scope):** Mở rộng dự án `hql_spark_bridge` hiện tại để bổ sung chế độ hoạt động thứ ba — **Legacy SQL Migration** — tự động chuyển đổi các script HiveQL `com_r_*` cũ sang các script PySpark `com_t_*` sẵn sàng cho production, sử dụng một pipeline điều khiển bằng metadata (metadata-driven) và nhận thức được các quy tắc nguồn (source-rule-aware).

**Phiên bản:** 1.0  
**Trạng thái:** Bản nháp để Engineering Review

---

## Mục lục (Table of Contents)

1. [Bối cảnh & Vấn đề (Context & Problem Statement)](#1-bối-cảnh--vấn-đề)
2. [Tổng quan Kiến trúc — Chế độ C](#2-tổng-quan-kiến-trúc--chế-độ-c)
3. [Thay đổi Cấu trúc Dự án](#3-thay-đổi-cấu-trúc-dự-án)
4. [Bước 1 — SQL Decomposer (Bộ bóc tách SQL)](#4-bước-1--sql-decomposer)
5. [Bước 2 — Metadata Processor (Bộ xử lý Metadata)](#5-bước-2--metadata-processor)
6. [Bước 3 — PySpark Generator (Bộ sinh code PySpark)](#6-bước-3--pyspark-generator)
7. [Hệ thống Cấu hình (Source Rules & Pipeline YAML)](#7-hệ-thống-cấu-hình)
8. [Các điểm tích hợp AI Agent](#8-các-điểm-tích-hợp-ai-agent)
9. [Giao diện CLI](#9-giao-diện-cli)
10. [Ví dụ thực tế chi tiết: com_r_k2_cif_alias](#10-ví-dụ-thực-tế-chi-tiết)
11. [Chiến lược Testing](#11-chiến-lược-testing)
12. [Sơ đồ Phụ thuộc (Dependency Map) & Module Contracts](#12-sơ-đồ-phụ-thuộc--module-contracts)

---

## 1. Bối cảnh & Vấn đề (Context & Problem Statement)

### Hệ thống hiện tại có gì

Dự án hiện đang hỗ trợ hai chế độ (modes):

| Chế độ (Mode) | Đầu vào (Input) | Đầu ra (Output) |
|---|---|---|
| **A — Pipeline Factory** | YAML pipeline spec + database nguồn | Các script DDL/DML cho từng layer |
| **B — SQL Transpiler** | File `.sql` HiveQL cũ | Script PySpark (SQL được bọc trong `spark.sql()`) |

### Hệ thống đang thiếu gì

Trình chuyển đổi (transpiler) của Chế độ B chỉ là chuyển đổi về mặt **cú pháp (syntactic)**: nó chuyển cú pháp HiveQL sang cú pháp Spark SQL nhưng không hiểu được **mục đích nghiệp vụ (business intent)** của một script cũ. Khi di cư (migrating) các script `com_r_*` của Datalake cũ sang script `com_t_*` của Datalake mới, cần thực hiện bốn bước biến đổi sâu hơn:

1. **Bóc tách cấu trúc (Structural decomposition)** — Một khối SQL nguyên khối (monolithic) dài 200 dòng phải được chia nhỏ thành các đoạn sub-SQL độc lập cho từng bảng tạm (temp table).
2. **Trích xuất schema (Schema extraction)** — Định nghĩa các cột phải được đọc từ file DDL đi kèm và được bổ sung thêm (enriched) các trường chuẩn mới.
3. **Lọc dựa trên Source-rule** — Không phải tất cả các bảng tạm trong script cũ đều cần được sinh lại (ví dụ: các hậu tố `_bk`, `_bf`, `_nw`, `_od` đối với source k2 sẽ bị bỏ qua hoàn toàn).
4. **Sinh DML theo Data Model** — Logic merge từ RAW vào COM phải tuân theo một template loading model cụ thể (từ Model 1–6B), chứ không phải là dịch nguyên xi logic SCD-2 cũ.

### Chế độ C mới

```
Đầu vào:  Legacy com_r_*.sql  (HiveQL, Datalake cũ)
Đầu ra:   com_t_*.py          (PySpark, Datalake mới)
          com_t_*.sql         (DDL — đã được enrich)
          {pipeline_id}.yaml  (Artifact metadata)
```

Chế độ C được triển khai như một **pipeline 3 bước**: Decompose (Bóc tách) → Process Metadata (Xử lý Metadata) → Generate (Sinh code).

---

## 2. Tổng quan Kiến trúc — Chế độ C

```
┌──────────────────────────────────────────────────────────────────┐
│                       ĐẦU VÀO (Chế độ C)                         │
│   com_r_k2_cif_alias.sql  +  com_r_k2_cif_alias.sql (DDL)        │
│   k2.yaml (source rules)                                         │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────┐
│  BƯỚC 1: SQL Decomposer        │
│  src/migration/decomposer.py   │
│                                │
│  - Bóc tách bằng sqlglot AST   │
│  - Nhận diện Temp table        │
│  - Trích xuất file-per-table   │
│  - Cô lập Main DML             │
└────────────┬───────────────────┘
             │  Tạo ra:
             │  processing_steps/{table}/r_k2_cif_alias_bk.sql
             │  processing_steps/{table}/r_k2_cif_alias_nw.sql  ...
             ▼
┌────────────────────────────────┐
│  BƯỚC 2: Metadata Processor    │
│  src/migration/metadata.py     │
│                                │
│  - Đọc DDL (trích xuất schema) │
│  - Lọc non_original_field      │
│  - Nhận diện Model type        │
│  - Nhận diện Key (rule→AI)     │
│  - Merge source rule           │
│  - Ghi Pipeline YAML           │
└────────────┬───────────────────┘
             │  Tạo ra:
             │  metadata/{pipeline_id}.yaml
             ▼
┌────────────────────────────────┐
│  BƯỚC 3: PySpark Generator     │
│  src/migration/generator.py    │
│                                │
│  - Sinh DDL (enriched)         │
│  - Sinh DML                    │
│    ├─ Simple mode (1 .py)      │
│    └─ Complex mode (N .py)     │
│  - Jinja2 model template       │
└────────────┬───────────────────┘
             │  Tạo ra:
             │  ddl/com_t_k2_cif_alias.sql
             │  dml/com_t_k2_cif_alias.py
             └──────────────────────────────
```

---

## 3. Thay đổi Cấu trúc Dự án

Thêm các file/thư mục sau vào cây thư mục hiện tại. Không xóa hay đổi tên bất kỳ thứ gì.

```
hql_spark_bridge/
│
├── configs/
│   └── source_rules/               ← MỚI: migration rules cho từng source
│       ├── k2.yaml
│       ├── mhbos.yaml
│       └── lms.yaml
│
├── src/
│   └── migration/                  ← MỚI: Toàn bộ package cho Chế độ C
│       ├── __init__.py
│       ├── decomposer.py           # Bước 1 — SQL → sub-SQLs
│       ├── metadata.py             # Bước 2 — Trích xuất metadata + ghi YAML
│       ├── key_detector.py         # Nhận diện Key (rule-based + AI fallback)
│       ├── model_detector.py       # Phân loại loading model type
│       ├── ddl_enricher.py         # Chỉnh sửa DDL (thêm các trường chuẩn mới)
│       └── generator.py            # Bước 3 — Sinh script PySpark
│
├── template/
│   └── migration/                  ← MỚI: Các template dành riêng cho migration
│       ├── model_3/
│       │   ├── com_t_dml.jinja     # Template PySpark DML cho Model 3
│       │   └── com_t_ddl.jinja     # Template DDL cho Model 3
│       ├── model_scd1/
│       │   └── com_t_dml.jinja
│       └── ... (mỗi model một thư mục)
│
├── docs/
│   └── datalake_old/               ← MỚI: Nơi chứa các script legacy đầu vào
│       └── com_r_k2_cif_alias.sql
│
├── output/                         ← MỚI: Tất cả artifact đầu ra của Chế độ C
│   └── {pipeline_id}/
│       ├── metadata/
│       │   └── {pipeline_id}.yaml
│       ├── processing_steps/
│       │   └── {base_table_name}/
│       │       ├── r_k2_cif_alias_bk.sql
│       │       └── ...
│       ├── ddl/
│       │   └── com_t_k2_cif_alias.sql
│       └── dml/
│           └── com_t_k2_cif_alias.py
│
└── main.py                         ← CẬP NHẬT: entry point `migrate()` mới
```

---

## 4. Bước 1 — SQL Decomposer (Bộ bóc tách SQL)

**Module:** `src/migration/decomposer.py`

### 4.1 Nhiệm vụ (Responsibilities)

- Nhận đường dẫn đến một file legacy HiveQL `.sql` nguyên khối.
- Chia nhỏ file đó thành các file SQL riêng biệt tương ứng với từng thao tác trên temp table.
- Cô lập phần logic INSERT/merge cuối cùng thành một artifact riêng.
- Trả về một object `DecomposedScript` có cấu trúc.

### 4.2 Data Contract

```python
# src/migration/decomposer.py

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import sqlglot
import sqlglot.expressions as exp

@dataclass
class TempTableBlock:
    name: str                    # ví dụ: "r_k2_cif_alias_bk"
    suffix: str                  # ví dụ: "_bk"
    raw_sql: str                 # Đoạn text SQL được trích xuất
    ast_nodes: list              # Các node AST của sqlglot cho block này
    operation: str               # "create_as_select" | "drop_create_insert" | "insert_only"

@dataclass
class DecomposedScript:
    source_name: str             # "k2"
    base_table: str              # "cif_alias"
    pipeline_id: str             # "com_r_k2_cif_alias"
    layer: str                   # "com" | "raw"
    temp_tables: list[TempTableBlock]
    main_sql: str                # Block INSERT INTO bảng đích (target table) cuối cùng
    raw_sql_full: str            # Nội dung gốc của file
```

### 4.3 Thuật toán (Algorithm)

```python
class SqlDecomposer:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules

    def decompose(self, sql_file: Path) -> DecomposedScript:
        """
        Parse và chia nhỏ một file HiveQL nguyên khối thành các block có tên.
        """
        content = sql_file.read_text(encoding="utf-8")
        # 1. Loại bỏ header comment block (các dòng bắt đầu bằng --)
        content = self._strip_header(content)
        # 2. Loại bỏ các command đặc thù của Hive (SOURCE ..., SET ...) 
        content = self._clean_hive_specific(content)
        # 3. Parse AST
        statements = sqlglot.parse(content, read="hive", error_level=sqlglot.ErrorLevel.WARN)
        # 4. Nhóm các câu lệnh theo từng temp table
        blocks = self._group_by_table(statements)
        # 5. Nhận diện khối lệnh INSERT chính (bảng đích, không phải bảng tạm)
        main_sql, temp_blocks = self._separate_main(blocks, content)
        # 6. Build các object TempTableBlock
        temp_table_objs = self._build_temp_table_objects(temp_blocks, content)

        source_name, base_table, layer = self._parse_filename(sql_file)
        return DecomposedScript(
            source_name=source_name,
            base_table=base_table,
            pipeline_id=sql_file.stem,       # "com_r_k2_cif_alias"
            layer=layer,
            temp_tables=temp_table_objs,
            main_sql=main_sql,
            raw_sql_full=content
        )
```

### 4.4 Logic Nhóm Bảng (Table Grouping Logic)

Thách thức chính: liên kết mỗi câu lệnh (`CREATE`, `DROP`, `INSERT`, `TRUNCATE`) với temp table mà nó thao tác. Sử dụng thuật toán sau:

```python
def _group_by_table(self, statements: list) -> dict[str, list]:
    """
    Trả về dict: { table_name: [stmt1, stmt2, ...] }
    """
    groups = {}
    for stmt in statements:
        # Trích xuất tên bảng từ bất kỳ loại câu lệnh nào
        table_name = self._extract_table_name(stmt)
        if table_name:
            groups.setdefault(table_name, []).append(stmt)
    return groups

def _extract_table_name(self, stmt) -> Optional[str]:
    """
    Hoạt động với CREATE TABLE, DROP TABLE, INSERT INTO TABLE, TRUNCATE TABLE.
    Trả về tên bảng không bao gồm schema prefix.
    """
    if isinstance(stmt, (exp.Create, exp.Drop)):
        return stmt.find(exp.Table).name
    elif isinstance(stmt, exp.Insert):
        return stmt.find(exp.Table).name
    elif isinstance(stmt, exp.TruncateTable):
        return stmt.find(exp.Table).name
    return None
```

### 4.5 Cô lập Main SQL (Main SQL Isolation)

"Main SQL" được định nghĩa là các câu lệnh tác động đến **bảng đích chính (primary target table)** (không phải bảng tạm). Bảng đích chính là bảng KHÔNG khớp với bất kỳ hậu tố (suffix) bảng tạm nào từ source rules VÀ được tham chiếu trong khối lệnh `INSERT INTO ... PARTITION` hoặc `INSERT OVERWRITE` cuối cùng.

```python
def _separate_main(self, blocks: dict, content: str) -> tuple[str, dict]:
    """
    Nhận diện bảng đích chính bằng phương pháp loại trừ:
    - Bất kỳ bảng nào có tên chứa hậu tố khớp với source_rules.temp_table_rules.*suffix là bảng tạm
    - Các bảng còn lại là ứng viên cho bảng đích
    - Bảng có khối lệnh INSERT INTO PARTITION cuối cùng = main
    """
    skip_suffixes = self._get_skip_suffixes()   # ["_bk","_bf","_nw","_od"] từ source rules
    temp_blocks = {}
    main_stmts = []
    for table_name, stmts in blocks.items():
        is_temp = any(table_name.endswith(sfx) for sfx in skip_suffixes)
        if is_temp:
            temp_blocks[table_name] = stmts
        else:
            main_stmts.extend(stmts)
    main_sql = "\n\n".join(s.sql(dialect="hive") for s in main_stmts)
    return main_sql, temp_blocks
```

### 4.6 Đầu ra: Ghi file (File Writing)

```python
class DecomposerWriter:
    def write(self, decomposed: DecomposedScript, output_root: Path):
        steps_dir = output_root / "processing_steps" / decomposed.base_table
        steps_dir.mkdir(parents=True, exist_ok=True)

        for block in decomposed.temp_tables:
            out_file = steps_dir / f"{block.name}.sql"
            out_file.write_text(block.raw_sql, encoding="utf-8")

        # Ghi riêng Main SQL để tham chiếu
        (steps_dir / "_main_dml.sql").write_text(decomposed.main_sql, encoding="utf-8")
```

### 4.7 Các Trường Hợp Đặc Biệt (Edge Cases)

| Trường hợp | Xử lý |
|---|---|
| Cặp `DROP TABLE IF EXISTS` + `CREATE TABLE IF NOT EXISTS` | Nhóm cả hai vào cùng một bảng |
| `TRUNCATE TABLE` + `INSERT INTO` sau CREATE | Nhóm cả 3 vào cùng một bảng (pattern của `_nw`/`_od`) |
| Comments nằm giữa các câu lệnh | `sqlglot` sẽ bỏ qua chúng; giữ lại text gốc cho output bằng cách dùng regex range extraction |
| `SOURCE /path/to/para_config.sql;` | Loại bỏ bằng regex trước khi parse: `re.sub(r'source\s+\S+;', '', content, flags=re.IGNORECASE)` |
| Inline comments `-- 2.1 create temp table` | Đính kèm vào khối raw SQL của câu lệnh tiếp theo |

---

## 5. Bước 2 — Metadata Processor (Bộ xử lý Metadata)

**Module:** `src/migration/metadata.py`

### 5.1 Nhiệm vụ (Responsibilities)

- Tìm và parse file `.sql` DDL đi kèm của pipeline.
- Trích xuất schema (tên cột + kiểu dữ liệu).
- Đánh dấu (annotate) các cột sử dụng rule `non_original_fields` từ source_rule.
- Nhận diện loại loading model.
- Nhận diện cột primary key.
- Merge các rule từ source rules để quyết định bảng tạm nào cần bỏ qua (skip).
- Serialize tất cả thành một file pipeline YAML.

### 5.2 Tìm DDL (DDL Resolution)

Vị trí file DDL tuân theo một quy ước. Cho đầu vào là `docs/datalake_old/com_r_k2_cif_alias.sql` (DML), DDL sẽ nằm tại:

```
docs/datalake_old/com_r_k2_cif_alias.sql  →  DDL: docs/datalake_old/com_r_k2_cif_alias.sql (variant DDL)
```

Tuy nhiên thực tế, DDL và DML thường là các file riêng biệt. Thứ tự tìm kiếm:

```python
def resolve_ddl_path(dml_path: Path) -> Optional[Path]:
    """
    Thứ tự ưu tiên tìm kiếm DDL:
    1. Cùng thư mục, cùng tên gốc (stem), cùng phần mở rộng (file DDL chứa 'CREATE TABLE' ở câu lệnh đầu tiên)
    2. Thư mục con tên 'ddl/' nằm cùng cấp, có cùng tên gốc
    3. Yêu cầu user cung cấp đường dẫn
    """
    candidates = [
        dml_path.parent / dml_path.name,              # cùng file (multi-statement)
        dml_path.parent / "ddl" / dml_path.name,
        dml_path.parent.parent / "ddl" / dml_path.name,
    ]
    for c in candidates:
        if c.exists() and _file_has_create_table(c):
            return c
    return None
```

### 5.3 Schema Extractor

Tái sử dụng module `src/core/ddl_parser.py` hiện có. Trả về `list[ColumnModel]`. Phần schema trong file YAML pipeline sẽ được build từ list này.

```python
from src.core.ddl_parser import DdlParser

class SchemaExtractor:
    def __init__(self, source_rules: dict):
        self.non_original_fields = set(
            source_rules.get("column_rules", {})
                        .get("non_original_fields", {})
                        .get("name", [])
        )

    def extract(self, ddl_path: Path) -> list[dict]:
        columns = DdlParser.parse_file(ddl_path)   # sử dụng module hiện có
        result = []
        for col in columns:
            remark = "non_original_field" if col.name in self.non_original_fields else None
            result.append({
                "name": col.name,
                "type": col.data_type,
                "remark": remark
            })
        return result
```

### 5.4 Model Type Detector

**Module:** `src/migration/model_detector.py`

Nhận diện Model type là một **bộ phân loại dựa trên luật (rule-based classifier)** hoạt động trên phần text của Main DML SQL. Các Model khớp với những pattern SQL cụ thể:

```python
MODEL_DETECTION_RULES = [
    {
        "model": "scd1",
        "description": "SCD Type 1 — INSERT OVERWRITE toàn bộ bảng + điều kiện NOT EXISTS",
        "required_patterns": [
            r"INSERT\s+OVERWRITE\s+TABLE",
            r"NOT\s+EXISTS",
        ],
        "excluded_patterns": [r"PARTITION\s*\("]
    },
    {
        "model": "3",
        "description": "Model 3 — Phát hiện Delta qua hash/updated date, không có partition",
        "required_patterns": [
            r"INSERT\s+INTO\s+TABLE.*?WHERE\s+NOT\s+EXISTS",
            r"record_updated_date|hash_value|updatets",
        ],
        "excluded_patterns": []
    },
    {
        "model": "5b",
        "description": "Model 5B — Ghi đè phân vùng theo khoảng ngày (Date-range)",
        "required_patterns": [
            r"INSERT\s+OVERWRITE\s+TABLE",
            r"PARTITION\s*\(",
            r"start_dt|end_dt|batch_date",
        ],
        "excluded_patterns": []
    },
    # ... thêm các rules khác cho models 1, 2a, 2b, 4, 5a, 6
]

class ModelDetector:
    def detect(self, main_sql: str, source_default: Optional[str] = None) -> str:
        """
        Trả về ID của model string ("3", "5b", "scd1", v.v.)
        Fallback về source_default nếu không có pattern nào khớp.
        """
        sql_upper = main_sql.upper()
        for rule in MODEL_DETECTION_RULES:
            required_ok = all(
                re.search(p, sql_upper, re.DOTALL | re.IGNORECASE)
                for p in rule["required_patterns"]
            )
            excluded_ok = not any(
                re.search(p, sql_upper, re.DOTALL | re.IGNORECASE)
                for p in rule["excluded_patterns"]
            )
            if required_ok and excluded_ok:
                return rule["model"]
        return source_default or "UNKNOWN"
```

**Bảng độ phủ nhận diện model:**

| Model | Key SQL Signals (Dấu hiệu SQL chính) |
|---|---|
| `scd1` | `INSERT OVERWRITE` (không partition) + subquery `NOT EXISTS` |
| `3` | `LEFT JOIN` qua key + so sánh delta `nvl() <>` + `INSERT INTO` |
| `5b` | `INSERT OVERWRITE` + `PARTITION (part_id)` + logic khoảng `start_dt/end_dt` |
| `2a` | `INSERT OVERWRITE TABLE` toàn bộ bảng (không partition, không NOT EXISTS) |
| `2b` | `INSERT OVERWRITE` + partition theo `year_month` |
| `4` | Xóa partition + insert chỉ trên các partition bị ảnh hưởng |
| `5a` | Chỉ append thêm dòng (Append-only insert) + điều kiện chặn `raw_incremental_etl_dt` |
| `6` | Reference upsert: `MERGE INTO` hoặc replace dựa trên key |

### 5.5 Key Detector (Nhận diện Key)

**Module:** `src/migration/key_detector.py`

Đây là module phức tạp nhất. Việc nhận diện Key sử dụng **các chiến lược ưu tiên giảm dần (cascading strategies)**:

**Chiến lược 1 — Ghi đè bằng Source Rule (ưu tiên cao nhất)**

Nếu `k2.yaml` cấu hình rõ một key cho pattern của bảng này, dùng nó luôn.

```yaml
# configs/source_rules/k2.yaml
table_key_rules:
  - pattern: "*_cif_alias"
    key: "cifaliasid"
  - pattern: "*_cif_*"
    key: "cifid"
```

**Chiến lược 2 — DDL Annotation (ưu tiên thứ hai)**

Nếu DDL chứa ràng buộc `PRIMARY KEY` hoặc một cột có tên `*id` là `NOT NULL` và đứng đầu:

```python
def _detect_from_ddl(self, columns: list[dict]) -> Optional[str]:
    # Heuristic 1: cột kết thúc bằng 'id' đứng ở vị trí đầu
    for col in columns:
        if col["name"].lower().endswith("id") and col.get("remark") is None:
            return col["name"]
    return None
```

**Chiến lược 3 — Phân tích Pattern của JOIN (ưu tiên thứ ba)**

Quét các block SQL temp table. Key chính là cột được sử dụng làm điều kiện JOIN giữa bảng backup (`_bk`) và bảng new-write (`_nw`):

```python
def _detect_from_join_analysis(self, decomposed: DecomposedScript) -> Optional[str]:
    """
    Tìm kiếm đoạn: ... LEFT JOIN {table}_bk o ON o.{col} = n.{col}
    Trích xuất {col} là key.
    """
    for block in decomposed.temp_tables:
        if block.suffix == "_nw":
            # Tìm các cột trong mệnh đề ON của AST
            for node in block.ast_nodes:
                joins = list(node.find_all(exp.Join))
                for join in joins:
                    on_clause = join.find(exp.On)
                    if on_clause:
                        eq_nodes = list(on_clause.find_all(exp.EQ))
                        if eq_nodes:
                            col = eq_nodes[0].left.name
                            return col
    return None
```

**Chiến lược 4 — So sánh Delta trong mệnh đề WHERE**

Mệnh đề `WHERE` để so sánh delta trong `_nw` luôn so sánh current vs backup bằng key:

```sql
where o.cifaliasid is null  ← key column
or nvl(cast(o.cifid ...) ...) <> ...
```

Điều kiện `IS NULL` trên bảng bên trái (left table) luôn nhắm vào cột key.

```python
def _detect_from_where_null_check(self, decomposed: DecomposedScript) -> Optional[str]:
    for block in decomposed.temp_tables:
        if block.suffix == "_nw":
            for node in block.ast_nodes:
                where_nodes = list(node.find_all(exp.Where))
                for w in where_nodes:
                    is_null_nodes = list(w.find_all(exp.Is))
                    for isn in is_null_nodes:
                        if isinstance(isn.right, exp.Null):
                            col = isn.left
                            if hasattr(col, 'name'):
                                return col.name
    return None
```

**Chiến lược 5 — AI Agent Fallback (cách cuối cùng)**

Nếu tất cả các chiến lược dựa trên rule đều thất bại, gọi Anthropic API với một prompt giới hạn chặt chẽ:

```python
def _detect_via_ai(self, decomposed: DecomposedScript, columns: list[dict]) -> str:
    """
    Chỉ được gọi KHI tất cả các chiến lược rule-based trả về None.
    Gửi một context tối thiểu để tránh lãng phí token.
    """
    col_names = [c["name"] for c in columns if c.get("remark") != "non_original_field"]
    
    # Chỉ trích xuất 50 dòng đầu tiên của bảng temp chứa nhiều thông tin nhất
    sample_sql = self._get_nw_table_sql(decomposed)[:2000]

    prompt = f"""Cho đoạn SQL snippet này từ một script ETL HiveQL:

```sql
{sample_sql}
```

Danh sách cột: {col_names}

Cột duy nhất nào là PRIMARY KEY (business identifier) được sử dụng trong mệnh đề JOIN ON?
Chỉ trả về tên cột, không trả về gì khác."""

    response = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=50,
        messages=[{"role": "user", "content": prompt}]
    )
    key = response.content[0].text.strip().lower()
    # Validate lại với tên các cột thực tế
    if key in [c["name"].lower() for c in columns]:
        return key
    raise KeyDetectionError(f"AI trả về key không xác định: {key}")
```

**Bộ Key Detector đầy đủ (cascading):**

```python
class KeyDetector:
    def detect(self, decomposed: DecomposedScript, columns: list, source_rules: dict) -> str:
        strategies = [
            ("source_rule_override", lambda: self._detect_from_source_rule(decomposed.base_table, source_rules)),
            ("ddl_heuristic",        lambda: self._detect_from_ddl(columns)),
            ("join_analysis",        lambda: self._detect_from_join_analysis(decomposed)),
            ("where_null_check",     lambda: self._detect_from_where_null_check(decomposed)),
            ("ai_fallback",          lambda: self._detect_via_ai(decomposed, columns)),
        ]
        for strategy_name, strategy_fn in strategies:
            result = strategy_fn()
            if result:
                logging.info(f"Đã nhận diện Key qua [{strategy_name}]: {result}")
                return result
        raise KeyDetectionError("Tất cả các chiến lược tìm key đều thất bại.")
```

### 5.6 Pipeline YAML Schema

File YAML đầu ra cho Chế độ C là phiên bản mở rộng của `sample_script_metadata.yaml`. Đặc tả chi tiết (Full spec):

```yaml
# output/{pipeline_id}/metadata/{pipeline_id}.yaml

pipeline_id: "com_r_k2_cif_alias"
layer: "com"
model_type: "3"              # Nhận diện bởi ModelDetector
source_name: "k2"            # Trích xuất từ tên file
target_table_name: "t_k2_cif_alias"   # com_r → com_t, loại bỏ prefix

# Lược đồ cột (từ DDL, được annotate bởi source rules)
columns:
  - name: "cifaliasid"
    type: "STRING"
    remark: null
  - name: "cifid"
    type: "STRING"
    remark: null
  - name: "etl_timestamp"
    type: "STRING"
    remark: "non_original_field"    # Từ k2.yaml column_rules
  - name: "start_dt"
    type: "STRING"
    remark: "non_original_field"
  - name: "end_dt"
    type: "STRING"
    remark: "non_original_field"
  - name: "part_id"
    type: "STRING"
    remark: "non_original_field"
  - name: "id_mark"
    type: "STRING"
    remark: null

# Key sử dụng cho logic JOIN / upsert
key: "cifaliasid"

# Pre-processing temp table blocks trích xuất từ script legacy
# Lọc qua source_rules.temp_table_rules (action: skip sẽ loại bỏ chúng)
pre_processing:
  - name: "r_k2_cif_alias_bk"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_bk.sql"
    action: "skip"            # Bê từ cấu hình phân giải rule
  - name: "r_k2_cif_alias_bf"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_bf.sql"
    action: "skip"
  - name: "r_k2_cif_alias_nw"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_nw.sql"
    action: "skip"
  - name: "r_k2_cif_alias_od"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_od.sql"
    action: "skip"

# Source rule đã được áp dụng
applied_source_rule: "configs/source_rules/k2.yaml"

# Các cột để so sánh Delta (không phải là key, cũng không phải cột hệ thống)
delta_columns:
  - "cifid"
  - "aliastype"
  - "aliasvalue"
  - "effectivefrom"
  - "effectiveto"
  - "updateuser"
  - "updatets"

# File DDL dùng để trích xuất schema
ddl_source: "docs/datalake_old/com_r_k2_cif_alias.sql"

# Vết nhận diện key (để audit)
key_detection_strategy: "join_analysis"
```

---

## 6. Bước 3 — PySpark Generator (Bộ sinh code PySpark)

**Module:** `src/migration/generator.py`

### 6.1 Nhiệm vụ (Responsibilities)

- Load file pipeline YAML sinh ra ở Bước 2.
- Generate một file DDL `.sql` đã được enriched.
- Generate một hoặc nhiều file DML PySpark `.py` dùng Jinja2 model templates.

### 6.2 DDL Enricher

**Module:** `src/migration/ddl_enricher.py`

DDL gốc của legacy sẽ bị chỉnh sửa như sau:

```
LOẠI BỎ: tất cả cột có remark == "non_original_field"
LOẠI BỎ: mệnh đề PARTITIONED BY (Datalake mới không còn dùng partition)
THÊM:    record_status, record_created_date, record_updated_date, hash_value, etl_dt, etl_timestamp
ĐỔI TÊN: bảng từ r_k2_cif_alias → t_k2_cif_alias
```

**Các trường chuẩn mới theo model:**

| Trường | Models yêu cầu | Kiểu dữ liệu |
|---|---|---|
| `record_status` | 3, scd1, all | `VARCHAR(10)` |
| `record_created_date` | 3, scd1, all | `TIMESTAMP` |
| `record_updated_date` | 3, scd1, all | `TIMESTAMP` |
| `hash_value` | 3 | `STRING` |
| `etl_dt` | all | `STRING` |
| `etl_timestamp` | all | `STRING` |

```python
# Các trường extra cho Model 3
MODEL_3_EXTRA_FIELDS = [
    {"name": "record_status",       "type": "VARCHAR(10)",  "comment": "A=Active"},
    {"name": "record_created_date", "type": "TIMESTAMP",    "comment": "First insert time"},
    {"name": "record_updated_date", "type": "TIMESTAMP",    "comment": "Last update time"},
    {"name": "hash_value",          "type": "STRING",       "comment": "MD5 of business columns"},
    {"name": "etl_dt",              "type": "STRING",       "comment": "Batch run date"},
    {"name": "etl_timestamp",       "type": "STRING",       "comment": "ETL processing timestamp"},
]
```

**DDL Jinja2 template** (`template/migration/model_3/com_t_ddl.jinja`):

```jinja
-- Purpose:    DDL/COM STATUS
-- Generated:  {{ generated_at }}
-- Source:     {{ pipeline_id }}

DROP TABLE IF EXISTS ${ com_schema }.{{ target_table_name }};

CREATE TABLE ${ com_schema }.{{ target_table_name }}(
    {% for col in columns if col.remark != "non_original_field" %}
    {{ col.name }} {{ col.type }} comment ''{% if not loop.last %},{% endif %}

    {% endfor %}
    -- Các trường Standard (Model {{ model_type }})
    {% for field in extra_fields %}
    ,{{ field.name }} {{ field.type }} comment '{{ field.comment }}'
    {% endfor %}
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);
```

### 6.3 DML Generator — Chế độ Simple

**Simple mode:** Tất cả business logic dồn hết vào một file `.py` duy nhất. Phần merge/upsert tuân theo đúng model template. Các block pre-processing không bị bỏ qua (not skipped) sẽ được render thành các câu lệnh `spark.sql()` đặt trước logic merge.

**DML Jinja2 template cho Model 3** (`template/migration/model_3/com_t_dml.jinja`):

```jinja
##  File Name   : com_{{ target_table_name }}
##  File Type   : DML
##  Model       : {{ model_type }}
##  Generated   : {{ generated_at }}
##  Source      : {{ pipeline_id }} (được migrate từ Datalake Cũ)

import os
import sys
sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
from etl_common_function import run_etl, set_parameter
from pyspark.sql.functions import current_timestamp, md5, concat_ws

source_name = "{{ source_name }}"
table_name  = "{{ base_table }}"

spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date
params = set_parameter(spark)

# ─── SETUP BẢNG TẠM (TEMP TABLE SETUP) ──────────────────────────────────────────
spark.sql(f"""DROP TABLE IF EXISTS {params["com_schema"]}.temp_{{ target_table_name }}_updated""")

spark.sql(f"""
CREATE TABLE {params["com_schema"]}.temp_{{ target_table_name }}_updated (
    {% for col in original_columns %}
    {{ col.name }} {{ col.type }}{% if not loop.last %},{% endif %}

    {% endfor %}
    ,record_status       VARCHAR(10)
    ,record_created_date TIMESTAMP
    ,record_updated_date TIMESTAMP
)
stored as parquet
tblproperties('parquet.compression'='SNAPPY', 'external.table.purge'='true')
""")

# ─── BƯỚC 1: Giữ nguyên các bản ghi không đổi ──────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_{{ target_table_name }}_updated
SELECT
    {% for col in original_columns %}
    {{ col.name }}{% if not loop.last %},{% endif %}

    {% endfor %}
    ,'A' AS record_status
    ,record_created_date
    ,record_updated_date
FROM {params["com_schema"]}.{{ target_table_name }} com
WHERE NOT EXISTS (
    SELECT 1 FROM {params["raw_schema"]}.{{ source_name }}_{{ base_table }} r
    WHERE r.etl_dt = '{batch_date}'
      AND r.{{ key }} = com.{{ key }}
)
""")

# ─── BƯỚC 2: Upsert các bản ghi đổi/mới ──────────────────────────────────────
spark.sql(f"""
INSERT INTO TABLE {params["com_schema"]}.temp_{{ target_table_name }}_updated
SELECT
    {% for col in original_columns %}
    r.{{ col.name }}{% if not loop.last %},{% endif %}

    {% endfor %}
    ,'A' AS record_status
    ,CASE WHEN com.{{ key }} IS NOT NULL THEN com.record_created_date
          ELSE current_timestamp() END AS record_created_date
    ,current_timestamp() AS record_updated_date
FROM {params["raw_schema"]}.{{ source_name }}_{{ base_table }} r
LEFT JOIN {params["com_schema"]}.{{ target_table_name }} com
    ON r.{{ key }} = com.{{ key }}
WHERE r.etl_dt = '{batch_date}'
  AND (
      com.{{ key }} IS NULL
      {% for col in delta_columns %}
      OR nvl(r.{{ col }}, '') <> nvl(com.{{ col }}, '')
      {% endfor %}
  )
""")

# ─── BƯỚC 3: Ghi đè (Overwrite) bảng đích ───────────────────────────────────────
spark.sql(f"""
INSERT OVERWRITE TABLE {params["com_schema"]}.{{ target_table_name }}
SELECT
    {% for col in original_columns %}
    {{ col.name }}{% if not loop.last %},{% endif %}

    {% endfor %}
    ,record_status
    ,record_created_date
    ,record_updated_date
    ,md5(concat_ws('|'
        {% for col in delta_columns %}
        , nvl(cast({{ col }} AS STRING), '')
        {% endfor %}
    )) AS hash_value
    ,'{batch_date}'       AS etl_dt
    ,current_timestamp()  AS etl_timestamp
FROM {params["com_schema"]}.temp_{{ target_table_name }}_updated
""")

spark.sql(f"""ANALYZE TABLE {params["com_schema"]}.{{ target_table_name }} COMPUTE STATISTICS""")
spark.stop()
```

### 6.4 DML Generator — Chế độ Complex

Trong complex mode, mỗi processing step được generate ra thành **một file Python riêng biệt**:

```
output/{pipeline_id}/dml/
├── 01_setup_temp_tables.py      # CREATE bảng tạm DDL
├── 02_keep_unchanged.py         # INSERT bản ghi không thay đổi
├── 03_upsert_changed.py         # INSERT bản ghi mới/thay đổi
├── 04_overwrite_target.py       # INSERT OVERWRITE bảng cuối
└── run_all.py                   # Orchestrator: chạy tuần tự các step
```

Mỗi file step tuân thủ cấu trúc boilerplate của PySpark nhưng chỉ chứa các lệnh `spark.sql()` tương ứng. Orchestrator `run_all.py` sẽ gọi chúng qua `subprocess.run()` hoặc qua một SparkSession dùng chung.

**Cờ chọn chế độ:**

```python
# main.py
migrate(
    input_sql="docs/datalake_old/com_r_k2_cif_alias.sql",
    source_rule="configs/source_rules/k2.yaml",
    output_mode="simple"    # hoặc "complex"
)
```

### 6.5 Xác định com_r → com_t (com_r → com_t Resolution)

Khi đầu vào là một script `com_r_*`, generator phải kiểm tra xem script `com_t_*` đã có tồn tại hay không:

```python
def resolve_target_script(input_path: Path, input_dir: Path) -> tuple[Path, str]:
    """
    Trả về tuple (script_to_use, target_prefix).
    
    Các quy tắc:
    1. Nếu input là com_r_*, tìm script com_t_* chung thư mục.
    2. Nếu com_t_* tồn tại → dùng nó, tên output sẽ giữ là com_t_*.
    3. Nếu com_t_* KHÔNG tồn tại → dùng com_r_* làm input, nhưng tên output = com_t_*.
    """
    stem = input_path.stem  # "com_r_k2_cif_alias"
    if stem.startswith("com_r_"):
        com_t_stem = stem.replace("com_r_", "com_t_", 1)
        com_t_path = input_dir / (com_t_stem + input_path.suffix)
        if com_t_path.exists():
            return com_t_path, com_t_stem
        else:
            return input_path, com_t_stem   # input=com_r, tên output=com_t
    return input_path, stem
```

---

## 7. Hệ thống Cấu hình (Configuration System)

### 7.1 Source Rule YAML — Đặc tả đầy đủ

**`configs/source_rules/{source_name}.yaml`**

```yaml
# configs/source_rules/k2.yaml

source: k2

# Default loading model áp dụng cho mọi pipeline thuộc source này.
# Có thể bị ghi đè bởi cấu hình trong pipeline YAML riêng lẻ hoặc bởi ModelDetector.
default_model: "3"

# Column rules: các cột sẽ bị đánh dấu là non-original (sẽ bị bỏ khỏi DDL đích)
column_rules:
  non_original_fields:
    name:
      - etl_timestamp
      - start_dt
      - end_dt
      - part_id

# Temp table rules: định nghĩa pattern của tên bảng tạm và cách xử lý
temp_table_rules:
  - suffix: "_bk"
    role: backup_table
    action: skip       # skip = loại bỏ khỏi pre_processing của script mới
  - suffix: "_bf"
    role: history_table
    action: skip
  - suffix: "_nw"
    role: new_write_table
    action: skip
  - suffix: "_od"
    role: unchange_table
    action: skip

# Tùy chọn (Optional): Các key rule riêng áp dụng cho một pattern tên bảng (dùng glob)
table_key_rules:
  - pattern: "*_cif_alias"
    key: "cifaliasid"
  # Sẽ thêm tiếp nếu phát hiện mới

# Các trường để add vào DDL theo từng model (được merge chung với global MODEL_EXTRA_FIELDS)
# Ghi đè cấu hình mặc định (global defaults) ở đây nếu source có quy ước khác
extra_ddl_fields:
  model_3:
    - name: "record_status"
      type: "VARCHAR(10)"
    - name: "record_created_date"
      type: "TIMESTAMP"
    - name: "record_updated_date"
      type: "TIMESTAMP"
    - name: "hash_value"
      type: "STRING"
    - name: "etl_dt"
      type: "STRING"
    - name: "etl_timestamp"
      type: "STRING"
```

### 7.2 Các model Pydantic cho Source Rules

```python
# src/models/source_rule_models.py

from pydantic import BaseModel
from typing import Optional

class NonOriginalFields(BaseModel):
    name: list[str]

class ColumnRules(BaseModel):
    non_original_fields: NonOriginalFields

class TempTableRule(BaseModel):
    suffix: str
    role: str
    action: str     # "skip" | "include" | "transform"

class TableKeyRule(BaseModel):
    pattern: str    # glob pattern
    key: str

class ExtraDdlField(BaseModel):
    name: str
    type: str
    comment: Optional[str] = None

class SourceRule(BaseModel):
    source: str
    default_model: str
    column_rules: ColumnRules
    temp_table_rules: list[TempTableRule]
    table_key_rules: list[TableKeyRule] = []
    extra_ddl_fields: dict[str, list[ExtraDdlField]] = {}
```

### 7.3 Migration Pipeline YAML — Pydantic Model

```python
# src/models/migration_models.py

from pydantic import BaseModel
from typing import Optional

class MigrationColumn(BaseModel):
    name: str
    type: str
    remark: Optional[str] = None

class PreProcessingStep(BaseModel):
    name: str
    file: str
    action: str = "include"   # "skip" | "include"

class MigrationPipelineConfig(BaseModel):
    pipeline_id: str
    layer: str
    model_type: str
    source_name: str
    target_table_name: str
    columns: list[MigrationColumn]
    key: str
    key_detection_strategy: str
    pre_processing: list[PreProcessingStep]
    delta_columns: list[str]
    applied_source_rule: str
    ddl_source: str
```

---

## 8. Các điểm tích hợp AI Agent (AI Agent Integration Points)

Nguyên tắc chỉ đạo: **Chỉ dùng AI khi cách giải quyết rule-based thất bại hoặc tỏ ra thua kém rõ rệt**.

### 8.1 Khi nào thì dùng AI

| Yêu cầu ra quyết định | Rule-based? | AI fallback? | Lý do (Rationale) |
|---|---|---|---|
| Cắt SQL thành các block | Có (AST grouping) | Không | Phân tích AST của sqlglot mang tính chất Deterministic (kết quả nhất quán) |
| Phát hiện model type | Có (pattern matching) | Không | Dấu hiệu SQL rất đáng tin cậy |
| Phát hiện primary key | Có (4 chiến lược) | Có (biện pháp cuối) | Đôi khi cấu trúc bảng dễ gây nhầm lẫn |
| Xác định các cột delta | Có (bỏ key + non_original) | Không | Là một phép toán trừ tập hợp thuần cơ học |
| Chuyển đổi logic custom phức tạp | Không | Có (cấu hình bật tay) | Các business logic đặc thù riêng nằm trong bảng tạm |

### 8.2 Hỗ trợ chuyển đổi logic phức tạp (Complex Transformation Assist - Optional)

Đối với các script có block pre-processing chứa quá trình xử lý phức tạp (như CASE/DECODE) chứ không chỉ đơn thuần là pass-through, thì thêm cờ `--ai-enrich` sẽ kích hoạt AI đi phân tích chi tiết:

```python
def ai_enrich_preprocessing(block: TempTableBlock, context: dict) -> str:
    """
    Sẽ được gọi khi action != "skip" VÀ logic của block SQL có CASE/DECODE phức tạp.
    Trả về câu lệnh PySpark tương đương với bước chuyển đổi của block đó.
    """
    prompt = f"""Chuyển đổi logic HiveQL temp table sau đây sang một transformation của PySpark DataFrame.
Hãy giữ nó dưới dạng spark.sql() — không dùng DataFrame API.
Thay thế các placeholder ${{variable}} thành biến python f-string: {{params["variable"]}}.

Input SQL:
```sql
{block.raw_sql}
```

CHỈ trả về kết quả là block code Python, không cần giải thích gì thêm."""
    # ... call API
```

**Việc gọi AI là có điều kiện (gated):**

```python
if not any_strategy_succeeded and args.ai_fallback:
    result = ai_enrich_preprocessing(block, context)
else:
    result = wrap_sql_in_spark(block.raw_sql)   # chỉ bọc lệnh trong spark.sql() đơn giản
```

### 8.3 Cấu hình gọi API

```python
# src/migration/ai_client.py

import anthropic

def get_client() -> anthropic.Anthropic:
    return anthropic.Anthropic()   # đọc ANTHROPIC_API_KEY từ env

MIGRATION_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS_KEY_DETECTION = 50
MAX_TOKENS_SQL_TRANSFORM = 2000
```

---

## 9. Giao diện CLI

### 9.1 Điểm nhập (Entry Point) — `main.py`

```python
# main.py — thêm vào các entry point đang có

def migrate(
        input_sql: str,
        source_rule: str,
        output_dir: str = "output",
        output_mode: str = "simple",  # "simple" | "complex"
        ai_fallback: bool = False,  # bật tính năng AI để detect key + transform enrichment
        dry_run: bool = False,  # in ra kế hoạch thay vì ghi file thực
):
    """
    Chế độ C: Migrate script com_r_* HiveQL cũ sang PySpark com_t_*.

    Tham số:
        input_sql:    Đường dẫn đến file .sql cũ (DML hoặc DDL+DML gộp chung)
        source_rule:  Đường dẫn đến source rule YAML
        output_dir:   Thư mục gốc của output
        output_mode:  "simple" = một file .py; "complex" = một .py cho từng step
        ai_fallback:  Dùng AI để detect key nếu rule-based thất bại
        dry_run:      In ra thông tin những file dự định sinh (không tạo file)
    """
    from migration.com.decomposer import ComSqlDecomposer, ComDecomposerWriter
    from migration.com.metadata import ComMetadataProcessor
    from migration.com.generator import ComPySparkGenerator
    from src.utils.source_rule_loader import load_source_rule

    source_rules = load_source_rule(source_rule)
    input_path = Path(input_sql)
    output_root = Path(output_dir)

    # Bước 1: Decompose
    decomposer = ComSqlDecomposer(source_rules)
    decomposed = decomposer.decompose(input_path)
    if not dry_run:
        ComDecomposerWriter().write(decomposed, output_root / decomposed.pipeline_id)

    # Bước 2: Metadata
    processor = ComMetadataProcessor(source_rules, ai_fallback=ai_fallback)
    pipeline_config = processor.process(decomposed, input_path, output_root)
    if not dry_run:
        processor.write_yaml(pipeline_config, output_root / decomposed.pipeline_id / "metadata")

    # Bước 3: Generate
    generator = ComPySparkGenerator(output_mode=output_mode)
    generator.generate(pipeline_config, output_root / decomposed.pipeline_id)

    print(f"✓ Migration thành công → {output_root / decomposed.pipeline_id}")
```

### 9.2 Các lệnh CLI (qua argparse hoặc Typer)

```bash
# Lệnh migrate cơ bản
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml

# Bật tính năng AI fallback cho key detection
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --ai-fallback

# Complex output mode (xuất một file riêng cho mỗi step)
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --output-mode complex

# Dry run (in ra plan)
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --dry-run

# Batch: tự động migrate tất cả file .sql trong một thư mục
python main.py migrate-batch \
  --input-dir docs/datalake_old/ \
  --source k2 \
  --source-rule configs/source_rules/k2.yaml
```

---

## 10. Ví dụ thực tế chi tiết: com_r_k2_cif_alias

### Đầu vào (Input)

```
docs/datalake_old/com_r_k2_cif_alias.sql   ← file đã gộp cả DDL+DML
configs/source_rules/k2.yaml
```

### Đầu ra của Bước 1 (Step 1 Output)

```
output/com_r_k2_cif_alias/processing_steps/cif_alias/
├── r_k2_cif_alias_bk.sql
├── r_k2_cif_alias_bf.sql
├── r_k2_cif_alias_nw.sql
├── r_k2_cif_alias_od.sql
└── _main_dml.sql
```

File `_main_dml.sql` chỉ chứa mỗi logic phân vùng cuối cùng:

```sql
-- Main DML đã được trích xuất ra
ALTER TABLE ${com_schema}.r_k2_cif_alias DROP IF EXISTS PARTITION (part_id = '${batch_yyyymm}');
INSERT INTO TABLE ${com_schema}.r_k2_cif_alias PARTITION (part_id)
SELECT * FROM ${com_schema}.r_k2_cif_alias_nw WHERE end_dt <> '${month_start}'
UNION ALL ...;
DROP TABLE ${com_schema}.r_k2_cif_alias_bk;
...
```

### Đầu ra của Bước 2 (Step 2 Output)

**Model detection:** Quét `_main_dml.sql` — tìm thấy `INSERT INTO TABLE ... PARTITION`, `start_dt`, `end_dt` → khớp ngay với model `5b`. Nhưng cũng đồng thời tìm thấy logic phát hiện delta bằng `LEFT JOIN ... nvl() <>` bên trong file `_nw.sql` → cấu hình source rule đã ghi đè bằng `default_model: "3"`. **Kết quả: model_type = "3".**

**Key detection:** Chiến lược 3 (JOIN analysis) phân tích trên `r_k2_cif_alias_nw.sql`:
```sql
LEFT JOIN ${com_schema}.r_k2_cif_alias_bk o ON o.cifaliasid = n.cifaliasid
```
→ **key = "cifaliasid"**, dựa trên chiến lược `join_analysis`.

**Kết quả YAML Output:** `output/com_r_k2_cif_alias/metadata/com_r_k2_cif_alias.yaml` — giống cấu trúc đã nêu trong phần 5.6.

### Đầu ra của Bước 3 (Step 3 Output)

**DDL (`output/com_r_k2_cif_alias/ddl/com_t_k2_cif_alias.sql`):**

```sql
DROP TABLE IF EXISTS ${com_schema}.t_k2_cif_alias;
CREATE TABLE ${com_schema}.t_k2_cif_alias(
    cifaliasid          STRING          comment ''
    ,cifid              STRING          comment ''
    ,aliastype          STRING          comment ''
    ,aliasvalue         STRING          comment ''
    ,effectivefrom      TIMESTAMP       comment ''
    ,effectiveto        TIMESTAMP       comment ''
    ,updateuser         BIGINT          comment ''
    ,updatets           TIMESTAMP       comment ''
    -- Các trường Standard (Model 3)
    ,record_status      VARCHAR(10)     comment 'A=Active'
    ,record_created_date TIMESTAMP      comment 'First insert time'
    ,record_updated_date TIMESTAMP      comment 'Last update time'
    ,hash_value         STRING          comment 'MD5 of business columns'
    ,etl_dt             STRING          comment 'Batch run date'
    ,etl_timestamp      STRING          comment 'ETL processing timestamp'
)
comment ''
stored as parquet
tblproperties(
    'parquet.compression'='SNAPPY'
    ,'external.table.purge'='true'
);
```

*Các trường đã bị loại bỏ:* `id_mark`, `start_dt`, `end_dt`, `etl_timestamp` (vì là non_original_field), `part_id`, câu lệnh khai báo `PARTITIONED BY`.

**DML (`output/com_r_k2_cif_alias/dml/com_t_k2_cif_alias.py`):**

Được render dựa trên `template/migration/model_3/com_t_dml.jinja` chứa các context variables sau:
- `original_columns` = [cifaliasid, cifid, aliastype, aliasvalue, effectivefrom, effectiveto, updateuser, updatets]
- `delta_columns` = [cifid, aliastype, aliasvalue, effectivefrom, effectiveto, updateuser, updatets]
- `key` = `cifaliasid`
- `target_table_name` = `t_k2_cif_alias`
- `source_name` = `k2`
- `base_table` = `cif_alias`

Đầu ra sinh ra hoàn toàn khớp với file `com_t_k2_cif_alias.py` (file tham khảo) do user cung cấp.

---

## 11. Chiến lược Testing

### Unit Tests

| Bài test | File test | Xác nhận điều gì |
|---|---|---|
| Decomposer chia tách đúng | `tests/migration/test_decomposer.py` | Trích xuất thành công 4 temp tables, cô lập được phần main DML |
| Model detector phân loại cả 8 models | `tests/migration/test_model_detector.py` | Mỗi SQL pattern của từng model trả về đúng ID |
| Thứ tự cascade của Key detector | `tests/migration/test_key_detector.py` | Source rule > DDL > JOIN > WHERE > AI |
| DDL enricher thêm đúng trường | `tests/migration/test_ddl_enricher.py` | Đã xóa cột non_original, và chèn đủ các cột mới |
| Serializer YAML dạng Round-trip | `tests/migration/test_metadata.py` | YAML được tạo ra sau đó parse ngược lại sẽ giống y hệt (Pydantic model) |

### Integration Test

```python
# tests/migration/test_end_to_end.py

def test_k2_cif_alias_full_pipeline():
    """
    Thực thi toàn bộ Mode C pipeline lên file input thực tế com_r_k2_cif_alias.sql.
    So sánh file đầu ra sinh ra với các file đáp án chuẩn (reference outputs).
    """
    migrate(
        input_sql="tests/fixtures/com_r_k2_cif_alias.sql",
        source_rule="tests/fixtures/k2.yaml",
        output_dir="tests/output_tmp",
        output_mode="simple",
        ai_fallback=False,
    )
    # Xác nhận DDL
    generated_ddl = Path("tests/output_tmp/com_r_k2_cif_alias/ddl/com_t_k2_cif_alias.sql").read_text()
    assert "t_k2_cif_alias" in generated_ddl
    assert "record_status" in generated_ddl
    assert "start_dt" not in generated_ddl      # đã loại bỏ non_original_field
    assert "PARTITIONED BY" not in generated_ddl

    # Xác nhận DML
    generated_dml = Path("tests/output_tmp/com_r_k2_cif_alias/dml/com_t_k2_cif_alias.py").read_text()
    assert "cifaliasid" in generated_dml
    assert "INSERT OVERWRITE TABLE" in generated_dml
    assert "hash_value" in generated_dml
```

### Notebook

Thêm `notebooks/test_migration_mode.ipynb` hướng dẫn và chạy thử từng cell cho ví dụ k2_cif_alias (tốt cho việc trình bày cho stakeholders).

---

## 12. Sơ đồ Phụ thuộc (Dependency Map) & Module Contracts

### Sơ đồ phụ thuộc các module (Module Dependency Graph)

```
main.migrate()
    │
    ├── SqlDecomposer.decompose()
    │       └── uses: sqlglot, source_rules
    │       returns: DecomposedScript
    │
    ├── MetadataProcessor.process()
    │       ├── DdlParser.parse_file()           [module có sẵn src/core/ddl_parser.py]
    │       ├── SchemaExtractor.extract()
    │       ├── ModelDetector.detect()
    │       ├── KeyDetector.detect()
    │       │       └── tuỳ chọn: AnthropicClient [module mới src/migration/ai_client.py]
    │       └── returns: MigrationPipelineConfig
    │
    └── PySparkGenerator.generate()
            ├── DdlEnricher.enrich()
            ├── Jinja2Environment.render()       [module có sẵn src/jinja/environment.py]
            └── ghi ra đĩa: các file .sql + .py
```

### Các dependencies mới (add vào `requirements.txt`)

```
# Đã có sẵn rồi (không cần thay đổi):
sqlglot>=23.0
pydantic>=2.0
jinja2>=3.1
pyyaml>=6.0
anthropic>=0.25    # đã được sử dụng từ trước ở nơi khác

# Không cần cài thêm thư viện nào cho Mode C core.
```

### Tích hợp với Code hiện tại

Chế độ C sẽ tái sử dụng các module hiện có sau đây mà không cần phải chỉnh sửa gì:

- `src/core/ddl_parser.py` — Chuyển từ file DDL → danh sách cột
- `src/jinja/environment.py` — Thiết lập môi trường Jinja2 (có chứa TypeResolver)
- `src/utils/yaml_hydrator.py` — Tiện ích đọc YAML (có thể lấy các thao tác đọc từ đây)
- `src/core/parser.py` — Logic của hàm `_strip_header()` và `_clean_hive_specific()` có thể trích xuất ra một tiện ích chung

---

## Trình tự Triển khai (Implementation Sequencing / Suggested Sprint Order)

**Sprint 1 (Khởi tạo Nền tảng)**
- `src/migration/decomposer.py` + unit tests
- `configs/source_rules/k2.yaml` + Model Pydantic `SourceRule`
- DecomposerWriter + Cấu trúc thư mục

**Sprint 2 (Xử lý Metadata)**
- `src/migration/model_detector.py` + tất cả luật cho 8 model
- `src/migration/key_detector.py` (cài các chiến lược 1–4, chưa cần AI)
- `src/migration/metadata.py` - bộ YAML writer
- Model Pydantic `MigrationPipelineConfig`

**Sprint 3 (Sinh Code)**
- `src/migration/ddl_enricher.py`
- `template/migration/model_3/com_t_ddl.jinja`
- `template/migration/model_3/com_t_dml.jinja`
- `src/migration/generator.py` (làm simple mode trước)
- Viết integration test (End-to-end) cho k2_cif_alias

**Sprint 4 (Hoàn thiện / Hardening)**
- Khả năng fallback dùng AI cho key detection (chiến lược thứ 5 của `key_detector.py`)
- Complex output mode cho generator
- Xây CLI batch migration
- Triển khai các model templates còn lại (scd1, 5b, 2a, 2b)
- Bổ sung cấu hình source rules cho các source khác (mhbos, lms, toms)