# HQL Spark Bridge - Data Pipeline Factory

> **A YAML-driven, template-based automation engine that transpiles HiveQL to PySpark
> and generates complete ETL pipeline scripts across a multi-layer Datalake architecture.**

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Core Modules](#core-modules)
  - [Discovery Layer](#1-discovery-layer)
  - [Hydration Engine](#2-hydration-engine)
  - [Code Generator](#3-code-generator)
  - [Transpilation Engine](#4-transpilation-engine)
- [Configuration System](#configuration-system)
  - [Pipeline YAML Spec](#pipeline-yaml-spec)
  - [Optimization Rules](#optimization-rules-yaml)
  - [Data Type Mapping](#data-type-mapping)
- [Datalake Loading Models](#datalake-loading-models)
- [Template System](#template-system)
- [Getting Started](#getting-started)
- [Tech Stack](#tech-stack)

---

## Overview

**HQL Spark Bridge** solves a critical bottleneck in enterprise data platform migrations:
the manual, error-prone conversion of legacy HiveQL ETL scripts into modern PySpark-based
data pipelines.

The engine operates in two modes:

| Mode | Description |
|---|---|
| **Pipeline Factory** | Read a YAML pipeline spec → auto-discover schema → generate full DDL/DML for Raw, Com, Cur, Unl layers |
| **SQL Transpiler** | Parse a `.sql` file (HiveQL) → transform via AST → output runnable PySpark script |

**Key design goals:**

- **Zero hardcoding** - all transformation rules, type mappings, and variables are YAML-configurable
- **Separation of concerns** - Discovery, Hydration, Generation, and Transpilation are fully decoupled
- **Strategy Pattern** - multiple transpilation strategies (Basic, Optimized) selectable at runtime
- **Single Source of Truth** - one `PipelineConfig` Pydantic model drives all code generation paths

---

## System Architecture
```

┌─────────────────────────────────────────────────────────────────┐
│                         INPUT LAYER                             │
│   Pipeline YAML  │  Source DB (MSSQL/API)  │  Legacy HiveQL     │
└────────┬─────────┴──────────┬──────────────┴──────┬─────────────┘
         │                    │                     │
         ▼                    ▼                     ▼
┌─────────────────┐  ┌──────────────────┐  ┌───────────────────────┐
│  YamlHydrator   │  │  MSSQLInspector  │  │   HiveScriptParser    │
│ (Pydantic model │  │  / ApiInspector  │  │   (sqlglot → AST)     │
│  validation)    │  │  (schema scan)   │  │                       │
└────────┬────────┘  └───────┬──────────┘  └───────────┬───────────┘
         │                   │                         │
         │            JSON Schema + SQL                │
         │                   │                         │
         ▼                   ▼                         ▼
┌─────────────────────────────────┐     ┌───────────────────────────┐
│         PipelineConfig          │     │    SqlConversionContext   │
│    (Pydantic - Central Model)   │     │  (Single Source of Truth) │
└────────────────┬────────────────┘     └──────────────┬────────────┘
                 │                                     │
          ┌──────┴──────┐                    ┌─────────┴──────────┐
          ▼             ▼                    ▼                    ▼
  ┌──────────────┐ ┌──────────┐   ┌───────────────────┐  ┌────────────────────┐
  │ HiveGenerator│ │(Roadmap) │   │BasicPySpark       │  │OptimizedPySpark    │
  │ (DDL + DML   │ │Airflow   │   │Transformer        │  │Transformer         │
  │  per layer)  │ │DAGGen    │   │(SQL wrapped in    │  │(AST rewrite →      │
  └──────┬───────┘ └──────────┘   │spark.sql())       │  │JDBC read / skip)   │
         │                        └────────┬──────────┘  └──────────┬─────────┘
         │                                 │                         │
         ▼                                 └─────────┬───────────────┘
┌──────────────────────┐                             ▼
│  Jinja2 Templates    │◄────────────────  JinjaRenderModel
│  (DDL/DML per layer  │                   (Processed output)
│   per model 1–6B)    │
└──────────────────────┘
         │
         ▼
  Generated SQL / PySpark Files
```
---

## Data Flow

### Mode A - Pipeline Factory (YAML → Generated Scripts)
```

1. User writes samples/pipelines/raw/m21_cashmovement.yaml
2. main.run_discovery()
   └─ MSSQLInspector connects to source DB
   └─ Extracts column schema → saves to samples/metadata/schemas/m21_cashmovement.json
   └─ Generates sample SELECT query → saves to samples/metadata/query/
3. main.generate_scripts('raw/m21_cashmovement')
   └─ YamlHydrator.load_and_hydrate()
      ├─ Reads YAML pipeline spec
      ├─ Merges with JSON schema (auto column detection)
      └─ Validates via PipelineConfig (Pydantic)
   └─ HiveGenerator.generate_bundle()
      ├─ Resolves template path: template/datalake_model_5b/hiveql_ddl/raw.jinja
      ├─ Renders DDL → samples/generated/datalake/ddl_*.sql
      └─ Renders DML → samples/generated/datalake/dml_*.sql
```
### Mode B - SQL Transpiler (HiveQL → PySpark)
```

1. HiveScriptParser.parse_file("raw_k2_bank.sql")
   └─ Strips header comments
   └─ Removes non-standard Hive commands (SOURCE ...)
   └─ sqlglot.parse(..., read="hive") → List[AST nodes]
   └─ Returns SqlConversionContext (immutable)
2. Transformer.transform(context)
   ├─ [BasicPySparkTransformer]
   │   ├─ Walk AST: replace exp.Parameter nodes → Python f-string vars
   │   ├─ Handle Literal nodes: SQL functions vs embedded vars
   │   └─ node.sql(dialect="spark") → translated query strings
   └─ [OptimizedPySparkTransformer]
       ├─ Per node: check configs/rules/optimizations/{source_name}.yaml
       ├─ Rule match → dispatch to handler (skip | generate_jdbc_read)
       └─ No match → fallback to BasicPySparkTransformer
3. render_template("pyspark/pyspark_basic.jinja", render_model)
   └─ Outputs complete .py file with Spark session boilerplate
```
---

## Project Structure
```bash
hql_spark_bridge/
├── configs/
│   ├── rules/
│   │   ├── optimizations/          # Per-source optimization rules (YAML)
│   │   │   ├── k2.yaml             # Rules: skip ALTER, skip DROP _et, JDBC read
│   │   │   └── mhbos.yaml
│   │   ├── data_type.yaml          # Canonical type mapping (MSSQL → Hive → Spark)
│   │   └── variable.yaml           # Variable mapping (${batch_date} → batch_date)
│   └── sources/
│       └── uat.yaml                # DB connection credentials per environment
│
├── src/
│   ├── context/
│   │   ├── sql_conversion_context.py   # SqlConversionContext + JinjaRenderModel
│   │   └── builder_context.py          # PipelineMetadata (discovery output)
│   ├── core/
│   │   ├── parser.py               # HiveScriptParser (sqlglot AST builder)
│   │   ├── ddl_parser.py           # DDL → Spark StructType converter
│   │   ├── ext_parser.py           # Pentaho Kettle XML parser
│   │   └── type_resolver.py        # Canonical type resolution from YAML
│   ├── discovery/
│   │   ├── base_inspector.py       # Abstract inspector (Spark or Native Python)
│   │   └── mssql_inspector.py      # MSSQL schema discovery (JDBC or pymssql)
│   ├── generators/
│   │   └── hive_generator.py       # DDL/DML file generator per layer
│   ├── jinja/
│   │   ├── environment.py          # Jinja2 env setup (StrictUndefined, TypeResolver global)
│   │   └── filters.py              # Custom filters (wrap_fstring_vars)
│   ├── models/
│   │   └── pipeline_models.py      # PipelineConfig, ColumnModel, TargetModel (Pydantic v2)
│   ├── transformers/
│   │   ├── base_transformer.py     # Abstract BaseSqlTransformer
│   │   ├── basic_pyspark_transformer.py    # Strategy A: SQL-wrapped PySpark
│   │   ├── raw_pyspark_transformer.py # Strategy B: Rule-based AST rewrite
│   │   └── utils.py                # Shared handlers: skip, generate_jdbc_read, is_rule_triggered
│   ├── spark_session_builder.py    # SparkSession factory (MSSQL + Postgres JARs)
│   └── utils/
│       └── yaml_hydrator.py        # YAML + JSON schema merge → PipelineConfig
│
├── template/
│   ├── datalake_model_{1..6b}/     # 8 loading model variants
│   │   ├── hiveql_ddl/             # DDL templates per layer (raw/com_t/com_m/cur)
│   │   └── hiveql_dml/             # DML templates per layer
│   ├── pyspark/
│   │   ├── pyspark_basic.jinja     # Basic PySpark output template
│   │   └── optimized_pyspark.jinja # Optimized (JDBC read) PySpark template
│   ├── extraction/
│   │   └── ext.jinja               # Pentaho Kettle XML generator
│   ├── orchestration/
│   │   └── airflow_dag.jinja       # (Roadmap) Airflow DAG generator
│   └── datahub/                    # AILab PostgreSQL staging/main/SP templates
│
├── docs/
│   └── Data Loading Methodology/   # Reference HiveQL per model (1, 2A, 2B, 3, 4, 5A, 5B, 6)
│
├── notebooks/                      # Jupyter testing notebooks per module
│   ├── test_builder.ipynb
│   ├── test_basic_transformer.ipynb
│   ├── test_optimized_transformer.ipynb
│   └── test_ddl_parser.ipynb
│
├── tests/                          # Unit tests (parser, translator, validator)
├── main.py                         # Entry point: run_discovery(), generate_scripts()
└── requirements.txt
```
---

## Core Modules

### 1. Discovery Layer

**`src/discovery/mssql_inspector.py`**

Automatically selects engine at runtime:

```python
# Spark available → JDBC engine (production)
df = spark.read.format("jdbc").option("dbtable", table_name).load()

# No Spark → Native pymssql (local dev / no VPN)
cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE ...")
```
```


Both engines normalize output through `TypeResolver.convert_datatype()` to ensure schema
consistency regardless of how the data was fetched.

**Output:** `samples/metadata/schemas/{source}_{table}.json`

```json
[
  {"name": "TransactionNo", "type": "IntegerType", "nullable": false},
  {"name": "TransactionDate", "type": "TimestampType", "nullable": false}
]
```


---

### 2. Hydration Engine

**`src/utils/yaml_hydrator.py`**

Merges user YAML overrides with auto-discovered JSON schema:

```
JSON Schema (base)     +    YAML column overrides    =    PipelineConfig
─────────────────────────────────────────────────────────────────────────
{name: "Amount",             expression: "CAST(...)"      Merged ColumnModel
 type: "StringType"}         is_partition: false           with expression set
```


**`src/models/pipeline_models.py`** - `PipelineConfig` (Pydantic v2)

Central validated model. Key computed fields:

```python
@computed_field
@property
def full_target_name(self) -> str:
    return f"{self.target.schema_name}.{self.target.table_name}"

@computed_field
def template_path_prefix(self) -> str:
    # e.g. "datalake_model_5b"
    return f"datalake_model_{self.target.transformation_model.lower()}"
```


---

### 3. Code Generator

**`src/generators/hive_generator.py`**

Resolves template path automatically from `PipelineConfig.template_path_prefix`:

```python
# template/datalake_model_5b/hiveql_ddl/raw.jinja
ddl_sql = render_template(f'{config.schema_name}.jinja', context, ddl_template_folder)

# template/datalake_model_5b/hiveql_dml/raw.jinja
dml_sql = render_template(f'{config.schema_name}.jinja', context, dml_template_folder)
```


**`ColumnModel.get_select_expression()`** - auto-applies transformation rules:

```python
# STRING columns in raw/com layer auto-get TRIM() + NULLIF()
# Numeric columns pass through unchanged
# Custom expression → rendered as-is
```


---

### 4. Transpilation Engine

**Parsing** - `src/core/parser.py`

```python
# Strips header comments, removes non-standard Hive commands (SOURCE ...)
# sqlglot.parse(content, read="hive") → List[exp.Expression] (AST nodes)
# Extracts source_name, table_name from filename convention: {layer}_{source}_{table}.sql
context = SqlConversionContext(
    ast_nodes=[...],
    source_name="k2",
    table_name="bank"
)
```


**Strategy Pattern** - Two transformer implementations:

| | `BasicPySparkTransformer` | `OptimizedPySparkTransformer` |
|---|---|---|
| **Goal** | Preserve SQL logic for readability | Maximize Spark performance |
| **Variable handling** | `${raw_schema}` → `{params["raw_schema"]}` | Same |
| **CREATE EXTERNAL TABLE** | Transpile to Spark SQL | Rewrite to `spark.read.format("jdbc")` |
| **ALTER TABLE DROP PARTITION** | Transpile | Skip (rule-based) |
| **Output** | `spark.sql(f"...")` | DataFrame API |
| **Config** | `variable.yaml` | `configs/rules/optimizations/{source}.yaml` |

**Rule Matching Engine** - `src/transformers/utils.py`

```python
# For each AST node, check YAML rules sequentially:
# 1. node_type matches? (Create / Drop / Alter / Insert)
# 2. table_name_suffix matches? (e.g., "_et")
# 3. table_properties match? (e.g., "STORED AS TEXTFILE")
# → First full match triggers action: "skip" or "generate_jdbc_read"
is_rule_triggered(rule, node, context) → bool
```


---

## Configuration System

### Pipeline YAML Spec

```yaml
# samples/pipelines/raw/m21_cashmovement.yaml
sources:
  - alias: CashMovement
    source_type: mssql
    connection_id: m21
    schema_path: CashMovement.json       # auto-resolved from discovery output

target:
  - table_type: raw
    schema_name: "${raw_schema}"
    table_name: m21_cashmovement
    transformation_model: "5b"           # → template/datalake_model_5b/
    partition_keys: [etl_dt]
    primary_keys: [TransactionNo]
    date_keys: [TransactionDate]

    transform_logic:
      - name: Amount
        expression: "CAST({{ Amount }} AS DECIMAL(20,4))"
```


### Optimization Rules (YAML)

```yaml
# configs/rules/optimizations/k2.yaml
rules:
  drop_partition:
    enabled: true
    trigger:
      node_type: "Alter"        # matches exp.Alter AST node
    action:
      type: "skip"              # → removed from output entirely

  create_external_table:
    enabled: true
    trigger:
      node_type: "Create"
      table_properties:
        - "STORED AS TEXTFILE"
    action:
      type: "generate_jdbc_read"
      parameters:
        schema: dbo
        port: 1433
        jdbc_url_variable: "jdbc_url"
        query_template: |
          SELECT
              {columns}
          FROM {source_db_table} (NOLOCK)
          WHERE 1 = 1
```


### Data Type Mapping

Three-tier canonical type system in `configs/rules/data_type.yaml`:

```
MSSQL native type → Canonical ID → Target dialect type
─────────────────────────────────────────────────────
"datetime"        → "TIMESTAMP"  → Hive: "TIMESTAMP"
                                 → Spark: "TimestampType"
                                 → Postgres: "TIMESTAMP"
                                 → Pentaho: "Date"

"decimal"         → "DECIMAL"    → Hive: "DECIMAL"
                                 → Spark: "DecimalType" (with p,s)
```


---

## Datalake Loading Models

The engine supports **8 production-proven loading patterns**, each with full DDL + DML
templates for Raw, Com_T, Com_M, and Cur layers:

| Model | Strategy | Use Case |
|---|---|---|
| **Model 1** | Full table reload + soft delete marking | Slowly changing dimension (full refresh) |
| **Model 2A** | `INSERT OVERWRITE` (truncate-reload) from RAW | Simple daily snapshot |
| **Model 2B** | Monthly snapshot partitioned by `year_month` | Point-in-time history (monthly) |
| **Model 3** | Delta detection via `record_updated_date` + hash compare | SCD Type 2 with history table |
| **Model 4** | Key-based upsert on impacted partitions | Partitioned incremental update |
| **Model 5A** | Append-only with `raw_incremental_etl_dt` dedup guard | Transactional append |
| **Model 5B** | Date-range based overwrite on impacted partitions | Time-series date-range refresh |
| **Model 6** | Reference-column-driven upsert | Reference/lookup tables |

Each model resolves to a folder: `template/datalake_model_{id}/{hiveql_ddl|hiveql_dml}/{layer}.jinja`

---

## Template System

**`src/jinja/environment.py`** - Jinja2 environment with:

- `StrictUndefined` - fails immediately on missing variables (no silent errors)
- `TypeResolver` injected as a global - callable from within any `.jinja` file
- `trim_blocks=True`, `lstrip_blocks=True` - clean whitespace control

**Usage in templates:**

```
{# Access TypeResolver to resolve canonical type → Hive dialect #}
{{ TypeResolver.get_datatype(col.data_type.upper(), dialect='hive') }}

{# Access ColumnModel method directly in template #}
{{ col.get_select_expression('raw') }}

{# Conditional partition block #}
{% for p_key in target.partition_keys %}
    {{ ", " if not loop.first else "" }}{{ p_key.upper() }} STRING COMMENT 'Partition Date'
{% endfor %}
```


---

## Getting Started

### Prerequisites

```shell script
Python 3.11+
Java 8+ (for PySpark)
```


### Installation

```shell script
git clone https://github.com/your-org/hql_spark_bridge.git
cd hql_spark_bridge

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```


### Step 1 - Configure Source Connection

```yaml
# configs/sources/uat.yaml
m21:
  type: mssql
  host: localhost
  port: 1433
  user: sa
  password: your_password
  database: Algo_Thu
```


### Step 2 - Run Schema Discovery

```python
# main.py
run_discovery("CashMovement", "m21")
# → Outputs: samples/metadata/schemas/m21_cashmovement.json
```


### Step 3 - Write Pipeline Config

```shell script
# Create: samples/pipelines/raw/m21_cashmovement.yaml
# (see Pipeline YAML Spec above)
```


### Step 4 - Generate Scripts

```python
generate_scripts('raw/m21_cashmovement')
# → Outputs:
#   samples/generated/datalake/ddl_raw_m21_cashmovement.sql
#   samples/generated/datalake/dml_raw_m21_cashmovement.sql
```


### Step 5 - Transpile HiveQL (optional)

```python
# notebooks/test_basic_transformer.ipynb
context = HiveScriptParser.parse_file("samples/input/dml/raw/raw_k2_bank.sql")
transformer = BasicPySparkTransformer(variable_mapping)
render_model = transformer.transform(context)
python_script = render_template("pyspark/pyspark_basic.jinja", render_model)
```


---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11 |
| SQL Parsing & AST | `sqlglot` (Hive + Spark dialects) |
| Data Validation | `pydantic` v2 |
| Template Engine | `jinja2` (StrictUndefined) |
| Config Format | YAML (`pyyaml`) |
| Distributed Compute | `pyspark` 4.x |
| DB Connectivity | `pymssql` (native) / Spark JDBC |
| Orchestration Target | Apache Hive 3.x on HDFS, Apache Spark 3.5+ |
| Orchestration (Roadmap) | Apache Airflow 2.x (DAG generator planned) |
| CI/CD | GitHub Actions (`.github/workflows/`) |
| Environment | WSL2 / Docker, Python virtualenv |
```
---

> **Note:** README này đã cover đầy đủ toàn bộ kiến trúc thực tế của project - từ discovery, hydration, code generation, đến transpilation engine và rule system. Bạn có thể attach thẳng vào CV hoặc GitHub repo mà không cần chỉnh sửa thêm gì đáng kể. Nếu muốn bổ sung badge (build status, Python version, license), tôi có thể thêm vào phần đầu.