import sys
import os
from pathlib import Path
import yaml

# 1. Ensure Python understands the project root so it can import modules from src
project_root = Path(os.getcwd())
# If running inside the notebooks folder, move up one level
if project_root.name == "notebooks":
    project_root = project_root.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.discovery.mssql_inspector import MSSQLInspector
from src.utils.yaml_hydrator import YamlHydrator
from src.generators.hive_generator import HiveGenerator
from src.core.type_resolver import TypeResolver


config_path = project_root / "configs" / "sources" / "uat.yaml"
pipline_folder_path = project_root / "samples" / "pipelines"
schema_folder_path = project_root / "samples" / "metadata" / "schemas"
query_folder_path = project_root / "samples" / "metadata" / "query"
template_root= project_root / "template"
output_root= project_root / "samples" / "generated" / "datalake"


def load_config(conn_name):
    """Read database configuration from YAML"""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config.get(conn_name)

def run_discovery(table_name, conn_name):
    # 1. Initialize environment
    db_config = load_config(conn_name)

    if not db_config:
        print(f"ERROR: Connection '{conn_name}' not found in database.yaml")
        return

    # 2. Initialize Inspector (Automatically select Engine: Spark or Native)
    inspector = MSSQLInspector(db_config)

    print(f"--- Starting Discovery for Table: {table_name} ---")

    # 3. Perform Schema scan
    try:
        schema = inspector.get_schema(table_name)
        query = inspector.get_sample_query(table_name)

        # 4. Save results to Central Metadata Repo
        inspector.save_metadata(conn_name, table_name, schema, query, str(schema_folder_path), str(query_folder_path))

        print(f"SUCCESS: Schema saved to {schema_folder_path}")
        print(f"SUCCESS: Schema saved to {query_folder_path}")
        print(f"Detected Columns: {len(schema)}")
        print(f"Sample Query: {query}")

    except Exception as e:
        print(f"FAILED: Could not inspect table {table_name}. Error: {e}")


def generate_scripts(pipeline_configconfig_path: str):
    # Initialize components
    hydrator = YamlHydrator(
        pipeline_folder=str(pipline_folder_path),
        schema_folder=str(schema_folder_path)
    )

    generator = HiveGenerator(
        template_root=str(template_root),
    )

    print("\n[Step 1] Hydrating Metadata...")
    try:
        pipeline_config_list = hydrator.load_and_hydrate(pipeline_configconfig_path)

        for pipeline_config in pipeline_config_list:
            print(f"Hydration Success! Pipeline: {pipeline_config.pipeline_id}")
            print(f"Target Table: {pipeline_config.full_target_name}")
            print(f"Template Path Prefix: {pipeline_config.template_path_prefix}")

            print("\n[Step 2] Generating Hive Scripts...")
            ddl_file, dml_file = generator.generate_bundle(pipeline_config, str(output_root))

            print(f"\n--- GENERATION SUCCESS ---")
            print(f"DDL File: {ddl_file}")
            print(f"DML File: {dml_file}")
        return pipeline_config_list

    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        return hydrator.load_and_hydrate(pipeline_configconfig_path)
