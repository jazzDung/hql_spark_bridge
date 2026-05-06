# HQL Spark Bridge — Mode C: Legacy SQL Migration
## Master Technical Specification & Implementation Guide

> **Scope:** Extension of the existing `hql_spark_bridge` project to add a third operating mode — **Legacy SQL Migration** — that automatically migrates `com_r_*` HiveQL scripts into production-ready `com_t_*` PySpark scripts, using a metadata-driven, source-rule-aware pipeline.

**Version:** 1.0  
**Status:** Draft for Engineering Review

---

## Table of Contents

1. [Context & Problem Statement](#1-context--problem-statement)
2. [Architecture Overview — Mode C](#2-architecture-overview--mode-c)
3. [Project Structure Changes](#3-project-structure-changes)
4. [Step 1 — SQL Decomposer](#4-step-1--sql-decomposer)
5. [Step 2 — Metadata Processor](#5-step-2--metadata-processor)
6. [Step 3 — PySpark Generator](#6-step-3--pyspark-generator)
7. [Configuration System (Source Rules & Pipeline YAML)](#7-configuration-system)
8. [AI Agent Integration Points](#8-ai-agent-integration-points)
9. [CLI Interface](#9-cli-interface)
10. [Full Worked Example: com_r_k2_cif_alias](#10-full-worked-example)
11. [Testing Strategy](#11-testing-strategy)
12. [Dependency Map & Module Contracts](#12-dependency-map--module-contracts)

---

## 1. Context & Problem Statement

### What exists today

The project already supports two modes:

| Mode | Input | Output |
|---|---|---|
| **A — Pipeline Factory** | YAML pipeline spec + source DB | DDL/DML scripts per layer |
| **B — SQL Transpiler** | Legacy HiveQL `.sql` file | PySpark script (SQL wrapped in `spark.sql()`) |

### What is missing

Mode B's transpiler is a **syntactic** translation: it converts HiveQL syntax to Spark SQL syntax but does not understand the **business intent** of a legacy script. When migrating `com_r_*` Datalake-old scripts to `com_t_*` Datalake-new scripts, four deeper transformations are required:

1. **Structural decomposition** — A monolithic 200-line SQL must be split into isolated sub-SQLs per temp table.
2. **Schema extraction** — Column definitions must be read from a companion DDL file and enriched with new standard fields.
3. **Source-rule-aware filtering** — Not all temp tables in the legacy script should be regenerated (e.g. `_bk`, `_bf`, `_nw`, `_od` for k2 source are to be skipped entirely).
4. **Model-aware DML generation** — The merge logic from RAW into COM must follow a specific loading model template (Models 1–6B), not be a literal translation of the old SCD-2 logic.

### The new Mode C

```
Input:  Legacy com_r_*.sql  (HiveQL, Datalake-old)
Output: com_t_*.py          (PySpark, Datalake-new)
        com_t_*.sql         (DDL — enriched)
        {pipeline_id}.yaml  (metadata artifact)
```

Mode C is implemented as a **three-step pipeline**: Decompose → Process Metadata → Generate.

---

## 2. Architecture Overview — Mode C

```
┌──────────────────────────────────────────────────────────────────┐
│                       INPUT (Mode C)                             │
│   com_r_k2_cif_alias.sql  +  com_r_k2_cif_alias.sql (DDL)       │
│   k2.yaml (source rules)                                         │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────┐
│  STEP 1: SQL Decomposer        │
│  src/migration/decomposer.py   │
│                                │
│  - sqlglot AST split           │
│  - Temp table detection        │
│  - File-per-table extraction   │
│  - Main DML isolation          │
└────────────┬───────────────────┘
             │  Produces:
             │  processing_steps/{table}/r_k2_cif_alias_bk.sql
             │  processing_steps/{table}/r_k2_cif_alias_nw.sql  ...
             ▼
┌────────────────────────────────┐
│  STEP 2: Metadata Processor    │
│  src/migration/metadata.py     │
│                                │
│  - DDL reader (schema extract) │
│  - non_original_field filter   │
│  - Model type detector         │
│  - Key detector (rule→AI)      │
│  - Source rule merge           │
│  - Pipeline YAML writer        │
└────────────┬───────────────────┘
             │  Produces:
             │  metadata/{pipeline_id}.yaml
             ▼
┌────────────────────────────────┐
│  STEP 3: PySpark Generator     │
│  src/migration/generator.py    │
│                                │
│  - DDL generator (enriched)    │
│  - DML generator               │
│    ├─ Simple mode (1 .py)      │
│    └─ Complex mode (N .py)     │
│  - Jinja2 model template       │
└────────────┬───────────────────┘
             │  Produces:
             │  ddl/com_t_k2_cif_alias.sql
             │  dml/com_t_k2_cif_alias.py
             └──────────────────────────────
```

---

## 3. Project Structure Changes

Add the following to the existing tree. Nothing is removed or renamed.

```
hql_spark_bridge/
│
├── configs/
│   └── source_rules/               ← NEW: per-source migration rules
│       ├── k2.yaml
│       ├── mhbos.yaml
│       └── lms.yaml
│
├── src/
│   └── migration/                  ← NEW: entire Mode C package
│       ├── __init__.py
│       ├── decomposer.py           # Step 1 — SQL → sub-SQLs
│       ├── metadata.py             # Step 2 — metadata extractor + YAML writer
│       ├── key_detector.py         # Key detection (rule-based + AI fallback)
│       ├── model_detector.py       # Loading model type classifier
│       ├── ddl_enricher.py         # DDL modifier (adds new standard fields)
│       └── generator.py            # Step 3 — PySpark script generator
│
├── template/
│   └── migration/                  ← NEW: migration-specific templates
│       ├── model_3/
│       │   ├── com_t_dml.jinja     # Model 3 PySpark DML template
│       │   └── com_t_ddl.jinja     # Model 3 DDL template
│       ├── model_scd1/
│       │   └── com_t_dml.jinja
│       └── ... (one folder per model)
│
├── docs/
│   └── datalake_old/               ← NEW: input legacy scripts go here
│       └── com_r_k2_cif_alias.sql
│
├── output/                         ← NEW: all Mode C output artifacts
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
└── main.py                         ← UPDATED: new `migrate()` entry point
```

---

## 4. Step 1 — SQL Decomposer

**Module:** `src/migration/decomposer.py`

### 4.1 Responsibilities

- Accept a path to a monolithic legacy HiveQL `.sql` file.
- Split it into one SQL file per logical temp table operation.
- Isolate the final INSERT/merge logic as a separate artifact.
- Return a structured `DecomposedScript` object.

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
    name: str                    # e.g. "r_k2_cif_alias_bk"
    suffix: str                  # e.g. "_bk"
    raw_sql: str                 # Extracted SQL text
    ast_nodes: list              # sqlglot AST nodes for this block
    operation: str               # "create_as_select" | "drop_create_insert" | "insert_only"

@dataclass
class DecomposedScript:
    source_name: str             # "k2"
    base_table: str              # "cif_alias"
    pipeline_id: str             # "com_r_k2_cif_alias"
    layer: str                   # "com" | "raw"
    temp_tables: list[TempTableBlock]
    main_sql: str                # The final INSERT INTO target table block
    raw_sql_full: str            # Original file content
```

### 4.3 Algorithm

```python
class SqlDecomposer:
    def __init__(self, source_rules: dict):
        self.source_rules = source_rules

    def decompose(self, sql_file: Path) -> DecomposedScript:
        """
        Parse and split a monolithic HiveQL file into named blocks.
        """
        content = sql_file.read_text(encoding="utf-8")
        # 1. Strip header comment block (lines starting with --)
        content = self._strip_header(content)
        # 2. Remove non-standard Hive commands (SOURCE ..., SET ...) 
        content = self._clean_hive_specific(content)
        # 3. Parse AST
        statements = sqlglot.parse(content, read="hive", error_level=sqlglot.ErrorLevel.WARN)
        # 4. Group statements by which temp table they belong to
        blocks = self._group_by_table(statements)
        # 5. Identify final INSERT block (target table, not a temp table)
        main_sql, temp_blocks = self._separate_main(blocks, content)
        # 6. Build TempTableBlock objects
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

### 4.4 Table Grouping Logic

The key challenge: associate every statement (`CREATE`, `DROP`, `INSERT`, `TRUNCATE`) with the temp table it operates on. Use this algorithm:

```python
def _group_by_table(self, statements: list) -> dict[str, list]:
    """
    Returns dict: { table_name: [stmt1, stmt2, ...] }
    """
    groups = {}
    for stmt in statements:
        # Extract table name from any statement type
        table_name = self._extract_table_name(stmt)
        if table_name:
            groups.setdefault(table_name, []).append(stmt)
    return groups

def _extract_table_name(self, stmt) -> Optional[str]:
    """
    Works for CREATE TABLE, DROP TABLE, INSERT INTO TABLE, TRUNCATE TABLE.
    Returns the unqualified table name (strips schema prefix if present).
    """
    if isinstance(stmt, (exp.Create, exp.Drop)):
        return stmt.find(exp.Table).name
    elif isinstance(stmt, exp.Insert):
        return stmt.find(exp.Table).name
    elif isinstance(stmt, exp.TruncateTable):
        return stmt.find(exp.Table).name
    return None
```

### 4.5 Main SQL Isolation

The "main SQL" is defined as statements targeting the **primary target table** (not a temp table). The primary target table is the one that does NOT match any temp table suffix from the source rules AND is referenced in the final `INSERT INTO ... PARTITION` or `INSERT OVERWRITE` block.

```python
def _separate_main(self, blocks: dict, content: str) -> tuple[str, dict]:
    """
    Identifies the primary target table by elimination:
    - Any table whose name suffix matches source_rules.temp_table_rules.*suffix is temp
    - Remaining tables are candidates for the target
    - The one with a final INSERT INTO PARTITION block = main
    """
    skip_suffixes = self._get_skip_suffixes()   # ["_bk","_bf","_nw","_od"] from source rules
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

### 4.6 Output: File Writing

```python
class DecomposerWriter:
    def write(self, decomposed: DecomposedScript, output_root: Path):
        steps_dir = output_root / "processing_steps" / decomposed.base_table
        steps_dir.mkdir(parents=True, exist_ok=True)

        for block in decomposed.temp_tables:
            out_file = steps_dir / f"{block.name}.sql"
            out_file.write_text(block.raw_sql, encoding="utf-8")

        # Write main SQL separately for reference
        (steps_dir / "_main_dml.sql").write_text(decomposed.main_sql, encoding="utf-8")
```

### 4.7 Edge Cases

| Case | Handling |
|---|---|
| `DROP TABLE IF EXISTS` + `CREATE TABLE IF NOT EXISTS` as pair | Group both to same table |
| `TRUNCATE TABLE` + `INSERT INTO` after CREATE | Group all 3 to same table (the `_nw`/`_od` pattern) |
| Comments between statements | `sqlglot` strips them; preserve original for output via regex range extraction |
| `SOURCE /path/to/para_config.sql;` | Strip with regex before parsing: `re.sub(r'source\s+\S+;', '', content, flags=re.IGNORECASE)` |
| Inline comments `-- 2.1 create temp table` | Attach to next statement's raw SQL block |

---

## 5. Step 2 — Metadata Processor

**Module:** `src/migration/metadata.py`

### 5.1 Responsibilities

- Locate and parse the companion DDL `.sql` file for the pipeline.
- Extract schema (column names + types).
- Annotate columns using source_rule `non_original_fields`.
- Detect the loading model type.
- Detect the primary key column(s).
- Merge source rules to determine which temp tables to skip.
- Serialize everything to a pipeline YAML file.

### 5.2 DDL Resolution

The DDL file location follows a convention. Given input `docs/datalake_old/com_r_k2_cif_alias.sql` (DML), the DDL is at:

```
docs/datalake_old/com_r_k2_cif_alias.sql  →  DDL: docs/datalake_old/com_r_k2_cif_alias.sql (DDL variant)
```

But in practice, DDL and DML are separate files. Resolution order:

```python
def resolve_ddl_path(dml_path: Path) -> Optional[Path]:
    """
    DDL resolution priority:
    1. Same directory, same stem, same extension (DDL file contains 'CREATE TABLE' as first statement)
    2. Sibling directory named 'ddl/' with same stem
    3. Ask user to supply path
    """
    candidates = [
        dml_path.parent / dml_path.name,              # same file (multi-statement)
        dml_path.parent / "ddl" / dml_path.name,
        dml_path.parent.parent / "ddl" / dml_path.name,
    ]
    for c in candidates:
        if c.exists() and _file_has_create_table(c):
            return c
    return None
```

### 5.3 Schema Extractor

Reuses existing `src/core/ddl_parser.py`. Returns `list[ColumnModel]`. The pipeline YAML schema section is built from this list.

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
        columns = DdlParser.parse_file(ddl_path)   # existing module
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

Model type detection is a **rule-based classifier** operating on the main DML SQL text. Models map to observable SQL patterns:

```python
MODEL_DETECTION_RULES = [
    {
        "model": "scd1",
        "description": "SCD Type 1 — INSERT OVERWRITE full table + NOT EXISTS guard",
        "required_patterns": [
            r"INSERT\s+OVERWRITE\s+TABLE",
            r"NOT\s+EXISTS",
        ],
        "excluded_patterns": [r"PARTITION\s*\("]
    },
    {
        "model": "3",
        "description": "Model 3 — Delta-detect via hash/updated date, no partition",
        "required_patterns": [
            r"INSERT\s+INTO\s+TABLE.*?WHERE\s+NOT\s+EXISTS",
            r"record_updated_date|hash_value|updatets",
        ],
        "excluded_patterns": []
    },
    {
        "model": "5b",
        "description": "Model 5B — Date-range partition overwrite",
        "required_patterns": [
            r"INSERT\s+OVERWRITE\s+TABLE",
            r"PARTITION\s*\(",
            r"start_dt|end_dt|batch_date",
        ],
        "excluded_patterns": []
    },
    # ... additional rules for models 1, 2a, 2b, 4, 5a, 6
]

class ModelDetector:
    def detect(self, main_sql: str, source_default: Optional[str] = None) -> str:
        """
        Returns the detected model ID string ("3", "5b", "scd1", etc.)
        Falls back to source_default if no pattern matches.
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

**Model detection coverage table:**

| Model | Key SQL Signals |
|---|---|
| `scd1` | `INSERT OVERWRITE` (no partition) + `NOT EXISTS` subquery |
| `3` | `LEFT JOIN` on key + `nvl() <>` delta comparison + `INSERT INTO` |
| `5b` | `INSERT OVERWRITE` + `PARTITION (part_id)` + `start_dt/end_dt` range logic |
| `2a` | `INSERT OVERWRITE TABLE` entire table (no partition, no NOT EXISTS) |
| `2b` | `INSERT OVERWRITE` + `year_month` partition |
| `4` | Partition-level delete + insert on impacted partitions only |
| `5a` | Append-only insert + `raw_incremental_etl_dt` guard |
| `6` | Reference upsert: `MERGE INTO` or key-driven replace |

### 5.5 Key Detector

**Module:** `src/migration/key_detector.py`

This is the most nuanced module. Key detection uses **cascading strategies**:

**Strategy 1 — Source Rule Override (highest priority)**

If `k2.yaml` explicitly declares a key for this table pattern, use it.

```yaml
# configs/source_rules/k2.yaml
table_key_rules:
  - pattern: "*_cif_alias"
    key: "cifaliasid"
  - pattern: "*_cif_*"
    key: "cifid"
```

**Strategy 2 — DDL Annotation (second priority)**

If the DDL contains a `PRIMARY KEY` constraint or a column named `*id` that is `NOT NULL` and first:

```python
def _detect_from_ddl(self, columns: list[dict]) -> Optional[str]:
    # Heuristic 1: column ending in 'id' that appears first
    for col in columns:
        if col["name"].lower().endswith("id") and col.get("remark") is None:
            return col["name"]
    return None
```

**Strategy 3 — JOIN Pattern Analysis (third priority)**

Scan the temp table SQL blocks. The key is the column used as the JOIN condition between the backup table (`_bk`) and the new-write table (`_nw`):

```python
def _detect_from_join_analysis(self, decomposed: DecomposedScript) -> Optional[str]:
    """
    Look for: ... LEFT JOIN {table}_bk o ON o.{col} = n.{col}
    Extract {col} as the key.
    """
    for block in decomposed.temp_tables:
        if block.suffix == "_nw":
            # Find ON clause columns in the AST
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

**Strategy 4 — WHERE Clause Delta Comparison**

The delta-detection `WHERE` clause in `_nw` always compares current vs backup on the key:

```sql
where o.cifaliasid is null  ← key column
or nvl(cast(o.cifid ...) ...) <> ...
```

The `IS NULL` check on the left table always targets the key column.

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

**Strategy 5 — AI Agent Fallback (last resort)**

If all rule-based strategies fail, invoke the Anthropic API with a tightly scoped prompt:

```python
def _detect_via_ai(self, decomposed: DecomposedScript, columns: list[dict]) -> str:
    """
    Called ONLY when all rule-based strategies return None.
    Sends minimal context to avoid token waste.
    """
    col_names = [c["name"] for c in columns if c.get("remark") != "non_original_field"]
    
    # Extract only the first 50 lines of the most informative temp table
    sample_sql = self._get_nw_table_sql(decomposed)[:2000]

    prompt = f"""Given this SQL snippet from a HiveQL ETL script:

```sql
{sample_sql}
```

Column list: {col_names}

Which single column is the PRIMARY KEY (business identifier) used in the JOIN ON clause?
Respond with ONLY the column name, nothing else."""

    response = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=50,
        messages=[{"role": "user", "content": prompt}]
    )
    key = response.content[0].text.strip().lower()
    # Validate against actual column names
    if key in [c["name"].lower() for c in columns]:
        return key
    raise KeyDetectionError(f"AI returned unrecognized key: {key}")
```

**Full cascading key detector:**

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
                logging.info(f"Key detected via [{strategy_name}]: {result}")
                return result
        raise KeyDetectionError("All key detection strategies exhausted.")
```

### 5.6 Pipeline YAML Schema

The output YAML for Mode C is an extension of `sample_script_metadata.yaml`. Full spec:

```yaml
# output/{pipeline_id}/metadata/{pipeline_id}.yaml

pipeline_id: "com_r_k2_cif_alias"
layer: "com"
model_type: "3"              # Detected by ModelDetector
source_name: "k2"            # Extracted from filename
target_table_name: "t_k2_cif_alias"   # com_r → com_t, drop prefix

# Column schema (from DDL, annotated with source rules)
columns:
  - name: "cifaliasid"
    type: "STRING"
    remark: null
  - name: "cifid"
    type: "STRING"
    remark: null
  - name: "etl_timestamp"
    type: "STRING"
    remark: "non_original_field"    # From k2.yaml column_rules
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

# Key used for JOIN / upsert logic
key: "cifaliasid"

# Pre-processing temp table blocks extracted from the legacy script
# Filtered by source_rules.temp_table_rules (action: skip removes them)
pre_processing:
  - name: "r_k2_cif_alias_bk"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_bk.sql"
    action: "skip"            # Copied from source_rule resolution
  - name: "r_k2_cif_alias_bf"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_bf.sql"
    action: "skip"
  - name: "r_k2_cif_alias_nw"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_nw.sql"
    action: "skip"
  - name: "r_k2_cif_alias_od"
    file: "output/com_r_k2_cif_alias/processing_steps/cif_alias/r_k2_cif_alias_od.sql"
    action: "skip"

# Source rule that was applied
applied_source_rule: "configs/source_rules/k2.yaml"

# Delta comparison columns (non-key, non-system columns)
delta_columns:
  - "cifid"
  - "aliastype"
  - "aliasvalue"
  - "effectivefrom"
  - "effectiveto"
  - "updateuser"
  - "updatets"

# DDL source used for schema extraction
ddl_source: "docs/datalake_old/com_r_k2_cif_alias.sql"

# Key detection trace (for auditability)
key_detection_strategy: "join_analysis"
```

---

## 6. Step 3 — PySpark Generator

**Module:** `src/migration/generator.py`

### 6.1 Responsibilities

- Load the pipeline YAML produced in Step 2.
- Generate an enriched DDL `.sql` file.
- Generate one or more PySpark `.py` DML files using Jinja2 model templates.

### 6.2 DDL Enricher

**Module:** `src/migration/ddl_enricher.py`

The legacy DDL is modified as follows:

```
REMOVE: all columns with remark == "non_original_field"
REMOVE: PARTITIONED BY clause (new Datalake has no partition)
ADD:    record_status, record_created_date, record_updated_date, hash_value, etl_dt, etl_timestamp
RENAME: table name from r_k2_cif_alias → t_k2_cif_alias
```

**New standard fields by model:**

| Field | Models requiring it | Type |
|---|---|---|
| `record_status` | 3, scd1, all | `VARCHAR(10)` |
| `record_created_date` | 3, scd1, all | `TIMESTAMP` |
| `record_updated_date` | 3, scd1, all | `TIMESTAMP` |
| `hash_value` | 3 | `STRING` |
| `etl_dt` | all | `STRING` |
| `etl_timestamp` | all | `STRING` |

```python
# Model 3 extra fields constant
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
    -- Standard fields (Model {{ model_type }})
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

### 6.3 DML Generator — Simple Mode

**Simple mode:** All business logic in a single `.py` file. The merge/upsert section follows the model template exactly. Pre-processing blocks that are not skipped are rendered as `spark.sql()` blocks before the merge.

**DML Jinja2 template for Model 3** (`template/migration/model_3/com_t_dml.jinja`):

```jinja
##  File Name   : com_{{ target_table_name }}
##  File Type   : DML
##  Model       : {{ model_type }}
##  Generated   : {{ generated_at }}
##  Source      : {{ pipeline_id }} (migrated from Datalake Old)

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

# ─── TEMP TABLE SETUP ────────────────────────────────────────────────────────
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

# ─── STEP 1: Keep unchanged records ──────────────────────────────────────────
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

# ─── STEP 2: Upsert changed/new records ──────────────────────────────────────
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

# ─── STEP 3: Overwrite target table ──────────────────────────────────────────
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

### 6.4 DML Generator — Complex Mode

In complex mode, each processing step is generated as a **separate Python file**:

```
output/{pipeline_id}/dml/
├── 01_setup_temp_tables.py      # CREATE temp table DDL
├── 02_keep_unchanged.py         # INSERT unchanged records
├── 03_upsert_changed.py         # INSERT new/changed records
├── 04_overwrite_target.py       # INSERT OVERWRITE final table
└── run_all.py                   # Orchestrator: calls all steps in order
```

Each step file follows the same PySpark boilerplate but contains only its relevant `spark.sql()` blocks. The `run_all.py` orchestrator uses `subprocess.run()` or a shared SparkSession passed as a parameter.

**Flag to select mode:**

```python
# main.py
migrate(
    input_sql="docs/datalake_old/com_r_k2_cif_alias.sql",
    source_rule="configs/source_rules/k2.yaml",
    output_mode="simple"    # or "complex"
)
```

### 6.5 com_r → com_t Resolution

When the input is a `com_r_*` script, the generator must check whether a `com_t_*` variant exists:

```python
def resolve_target_script(input_path: Path, input_dir: Path) -> tuple[Path, str]:
    """
    Returns (script_to_use, target_prefix).
    
    Rules:
    1. If input is com_r_*, look for com_t_* in same directory.
    2. If com_t_* exists → use com_t_*, output name remains com_t_*
    3. If com_t_* does NOT exist → use com_r_* as input, output name = com_t_*
    """
    stem = input_path.stem  # "com_r_k2_cif_alias"
    if stem.startswith("com_r_"):
        com_t_stem = stem.replace("com_r_", "com_t_", 1)
        com_t_path = input_dir / (com_t_stem + input_path.suffix)
        if com_t_path.exists():
            return com_t_path, com_t_stem
        else:
            return input_path, com_t_stem   # input=com_r, output name=com_t
    return input_path, stem
```

---

## 7. Configuration System

### 7.1 Source Rule YAML — Full Spec

**`configs/source_rules/{source_name}.yaml`**

```yaml
# configs/source_rules/k2.yaml

source: k2

# Default loading model for all pipelines from this source.
# Can be overridden by individual pipeline YAML or ModelDetector result.
default_model: "3"

# Column rules: columns to annotate as non-original (will be excluded from target DDL)
column_rules:
  non_original_fields:
    name:
      - etl_timestamp
      - start_dt
      - end_dt
      - part_id

# Temp table rules: define temp table name patterns and how to handle them
temp_table_rules:
  - suffix: "_bk"
    role: backup_table
    action: skip       # skip = do not include in pre_processing of new script
  - suffix: "_bf"
    role: history_table
    action: skip
  - suffix: "_nw"
    role: new_write_table
    action: skip
  - suffix: "_od"
    role: unchange_table
    action: skip

# Optional: explicit key rules per table name pattern (glob matching)
table_key_rules:
  - pattern: "*_cif_alias"
    key: "cifaliasid"
  # Add more as discovered

# Fields to add to DDL per model (merged with global MODEL_EXTRA_FIELDS)
# Override global defaults here if source has different conventions
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

### 7.2 Pydantic Models for Source Rules

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

## 8. AI Agent Integration Points

The guiding principle: **use AI only when rule-based approaches are exhausted or demonstrably inferior**.

### 8.1 When to Use AI

| Decision | Rule-based? | AI fallback? | Rationale |
|---|---|---|---|
| Split SQL into blocks | Yes (AST grouping) | No | Deterministic with sqlglot |
| Detect model type | Yes (pattern matching) | No | Observable SQL signatures are reliable |
| Detect primary key | Yes (4 strategies) | Yes (last resort) | Occasionally ambiguous table structure |
| Generate delta columns | Yes (exclude key + non_original) | No | Mechanical set subtraction |
| Transform complex custom logic | No | Yes (explicit) | Custom business logic in temp tables |

### 8.2 Complex Transformation Assist (Optional)

For scripts where pre-processing blocks have complex business transformations (not just pass-through), an optional `--ai-enrich` flag triggers a focused AI analysis:

```python
def ai_enrich_preprocessing(block: TempTableBlock, context: dict) -> str:
    """
    Called when action != "skip" AND block SQL has complex CASE/DECODE logic.
    Returns a PySpark-friendly equivalent of the block's transformation.
    """
    prompt = f"""Convert this HiveQL temp table logic to a PySpark DataFrame transformation.
Keep it as spark.sql() — do not use DataFrame API.
Replace ${{variable}} placeholders with Python f-string format: {{params["variable"]}}.

Input SQL:
```sql
{block.raw_sql}
```

Output ONLY the Python code block, no explanation."""
    # ... call API
```

**AI call is gated:**

```python
if not any_strategy_succeeded and args.ai_fallback:
    result = ai_enrich_preprocessing(block, context)
else:
    result = wrap_sql_in_spark(block.raw_sql)   # simple wrap
```

### 8.3 API Call Configuration

```python
# src/migration/ai_client.py

import anthropic

def get_client() -> anthropic.Anthropic:
    return anthropic.Anthropic()   # reads ANTHROPIC_API_KEY from env

MIGRATION_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS_KEY_DETECTION = 50
MAX_TOKENS_SQL_TRANSFORM = 2000
```

---

## 9. CLI Interface

### 9.1 Entry Point — `main.py`

```python
# main.py — add to existing entry points

def migrate(
    input_sql: str,
    source_rule: str,
    output_dir: str = "output",
    output_mode: str = "simple",    # "simple" | "complex"
    ai_fallback: bool = False,      # enable AI for key detection + transform enrichment
    dry_run: bool = False,          # print plan but don't write files
):
    """
    Mode C: Migrate a legacy com_r_* HiveQL script to com_t_* PySpark.

    Args:
        input_sql:    Path to legacy .sql file (DML or combined DDL+DML)
        source_rule:  Path to source rule YAML
        output_dir:   Root output directory
        output_mode:  "simple" = one .py file; "complex" = one .py per step
        ai_fallback:  Use AI for key detection if rule-based fails
        dry_run:      Print what would be generated without writing files
    """
    from src.migration.decomposer import SqlDecomposer, DecomposerWriter
    from src.migration.metadata import MetadataProcessor
    from src.migration.generator import PySparkGenerator
    from src.utils.source_rule_loader import load_source_rule

    source_rules = load_source_rule(source_rule)
    input_path = Path(input_sql)
    output_root = Path(output_dir)

    # Step 1: Decompose
    decomposer = SqlDecomposer(source_rules)
    decomposed = decomposer.decompose(input_path)
    if not dry_run:
        DecomposerWriter().write(decomposed, output_root / decomposed.pipeline_id)

    # Step 2: Metadata
    processor = MetadataProcessor(source_rules, ai_fallback=ai_fallback)
    pipeline_config = processor.process(decomposed, input_path, output_root)
    if not dry_run:
        processor.write_yaml(pipeline_config, output_root / decomposed.pipeline_id / "metadata")

    # Step 3: Generate
    generator = PySparkGenerator(output_mode=output_mode)
    generator.generate(pipeline_config, output_root / decomposed.pipeline_id)

    print(f"✓ Migration complete → {output_root / decomposed.pipeline_id}")
```

### 9.2 CLI Commands (via argparse or Typer)

```bash
# Basic migration
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml

# With AI fallback for key detection
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --ai-fallback

# Complex output mode (one file per step)
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --output-mode complex

# Dry run (print plan only)
python main.py migrate \
  --input docs/datalake_old/com_r_k2_cif_alias.sql \
  --source-rule configs/source_rules/k2.yaml \
  --dry-run

# Batch: migrate all .sql files in a directory
python main.py migrate-batch \
  --input-dir docs/datalake_old/ \
  --source k2 \
  --source-rule configs/source_rules/k2.yaml
```

---

## 10. Full Worked Example

### Input

```
docs/datalake_old/com_r_k2_cif_alias.sql   ← combined DDL+DML
configs/source_rules/k2.yaml
```

### Step 1 Output

```
output/com_r_k2_cif_alias/processing_steps/cif_alias/
├── r_k2_cif_alias_bk.sql
├── r_k2_cif_alias_bf.sql
├── r_k2_cif_alias_nw.sql
├── r_k2_cif_alias_od.sql
└── _main_dml.sql
```

The `_main_dml.sql` contains only the final partition logic:

```sql
-- Extracted main DML
ALTER TABLE ${com_schema}.r_k2_cif_alias DROP IF EXISTS PARTITION (part_id = '${batch_yyyymm}');
INSERT INTO TABLE ${com_schema}.r_k2_cif_alias PARTITION (part_id)
SELECT * FROM ${com_schema}.r_k2_cif_alias_nw WHERE end_dt <> '${month_start}'
UNION ALL ...;
DROP TABLE ${com_schema}.r_k2_cif_alias_bk;
...
```

### Step 2 Output

**Model detection:** Scans `_main_dml.sql` — finds `INSERT INTO TABLE ... PARTITION`, `start_dt`, `end_dt` → initial match for `5b`. But also finds the delta-detection logic with `LEFT JOIN ... nvl() <>` in `_nw.sql` → overridden by source rule `default_model: "3"`. **Result: model_type = "3".**

**Key detection:** Strategy 3 (JOIN analysis) on `r_k2_cif_alias_nw.sql`:
```sql
LEFT JOIN ${com_schema}.r_k2_cif_alias_bk o ON o.cifaliasid = n.cifaliasid
```
→ **key = "cifaliasid"**, via `join_analysis` strategy.

**Output:** `output/com_r_k2_cif_alias/metadata/com_r_k2_cif_alias.yaml` — as shown in section 5.6.

### Step 3 Output

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
    -- Standard fields (Model 3)
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

*Removed:* `id_mark`, `start_dt`, `end_dt`, `etl_timestamp` (non_original_field), `part_id`, `PARTITIONED BY` clause.

**DML (`output/com_r_k2_cif_alias/dml/com_t_k2_cif_alias.py`):**

Rendered from `template/migration/model_3/com_t_dml.jinja` with:
- `original_columns` = [cifaliasid, cifid, aliastype, aliasvalue, effectivefrom, effectiveto, updateuser, updatets]
- `delta_columns` = [cifid, aliastype, aliasvalue, effectivefrom, effectiveto, updateuser, updatets]
- `key` = `cifaliasid`
- `target_table_name` = `t_k2_cif_alias`
- `source_name` = `k2`
- `base_table` = `cif_alias`

The generated file matches the reference `com_t_k2_cif_alias.py` provided by the user.

---

## 11. Testing Strategy

### Unit Tests

| Test | File | What it verifies |
|---|---|---|
| Decomposer splits statements correctly | `tests/migration/test_decomposer.py` | 4 temp tables extracted, main DML isolated |
| Model detector classifies all 8 models | `tests/migration/test_model_detector.py` | Each model's SQL pattern triggers correct ID |
| Key detector cascade order | `tests/migration/test_key_detector.py` | Source rule > DDL > JOIN > WHERE > AI |
| DDL enricher adds correct fields | `tests/migration/test_ddl_enricher.py` | non_original columns removed, new fields added |
| YAML serializer round-trip | `tests/migration/test_metadata.py` | Written YAML re-parses to same Pydantic model |

### Integration Test

```python
# tests/migration/test_end_to_end.py

def test_k2_cif_alias_full_pipeline():
    """
    Full Mode C pipeline on the real com_r_k2_cif_alias.sql input.
    Compares generated files against reference outputs.
    """
    migrate(
        input_sql="tests/fixtures/com_r_k2_cif_alias.sql",
        source_rule="tests/fixtures/k2.yaml",
        output_dir="tests/output_tmp",
        output_mode="simple",
        ai_fallback=False,
    )
    # Assert DDL
    generated_ddl = Path("tests/output_tmp/com_r_k2_cif_alias/ddl/com_t_k2_cif_alias.sql").read_text()
    assert "t_k2_cif_alias" in generated_ddl
    assert "record_status" in generated_ddl
    assert "start_dt" not in generated_ddl      # non_original_field removed
    assert "PARTITIONED BY" not in generated_ddl

    # Assert DML
    generated_dml = Path("tests/output_tmp/com_r_k2_cif_alias/dml/com_t_k2_cif_alias.py").read_text()
    assert "cifaliasid" in generated_dml
    assert "INSERT OVERWRITE TABLE" in generated_dml
    assert "hash_value" in generated_dml
```

### Notebook

Add `notebooks/test_migration_mode.ipynb` with cell-by-cell walkthrough of the k2_cif_alias example, suitable for demonstrating to stakeholders.

---

## 12. Dependency Map & Module Contracts

### Module Dependency Graph

```
main.migrate()
    │
    ├── SqlDecomposer.decompose()
    │       └── uses: sqlglot, source_rules
    │       returns: DecomposedScript
    │
    ├── MetadataProcessor.process()
    │       ├── DdlParser.parse_file()           [existing src/core/ddl_parser.py]
    │       ├── SchemaExtractor.extract()
    │       ├── ModelDetector.detect()
    │       ├── KeyDetector.detect()
    │       │       └── optional: AnthropicClient [new src/migration/ai_client.py]
    │       └── returns: MigrationPipelineConfig
    │
    └── PySparkGenerator.generate()
            ├── DdlEnricher.enrich()
            ├── Jinja2Environment.render()       [existing src/jinja/environment.py]
            └── writes: .sql + .py files
```

### New Dependencies (add to `requirements.txt`)

```
# Already present (no change needed):
sqlglot>=23.0
pydantic>=2.0
jinja2>=3.1
pyyaml>=6.0
anthropic>=0.25    # already used elsewhere

# No new packages required for Mode C core.
```

### Integration with Existing Code

Mode C reuses these existing modules without modification:

- `src/core/ddl_parser.py` — DDL → column list parsing
- `src/jinja/environment.py` — Jinja2 environment with TypeResolver
- `src/utils/yaml_hydrator.py` — YAML loading utilities (read patterns from here)
- `src/core/parser.py` — `_strip_header()` and `_clean_hive_specific()` logic can be extracted to a shared utility

---

## Implementation Sequencing (Suggested Sprint Order)

**Sprint 1 (Foundation)**
- `src/migration/decomposer.py` + unit tests
- `configs/source_rules/k2.yaml` + `SourceRule` Pydantic model
- DecomposerWriter + directory structure

**Sprint 2 (Metadata)**
- `src/migration/model_detector.py` + all 8 model rules
- `src/migration/key_detector.py` (strategies 1–4, no AI yet)
- `src/migration/metadata.py` YAML writer
- `MigrationPipelineConfig` Pydantic model

**Sprint 3 (Generation)**
- `src/migration/ddl_enricher.py`
- `template/migration/model_3/com_t_ddl.jinja`
- `template/migration/model_3/com_t_dml.jinja`
- `src/migration/generator.py` (simple mode first)
- End-to-end integration test with k2_cif_alias

**Sprint 4 (Hardening)**
- AI fallback key detection (`key_detector.py` strategy 5)
- Complex output mode generator
- Batch migration CLI
- Remaining model templates (scd1, 5b, 2a, 2b)
- Additional source rules (mhbos, lms, toms)