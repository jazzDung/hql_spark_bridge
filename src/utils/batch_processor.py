# src/utils/batch_processor.py

from pathlib import Path
import sys

# Add the project root to the system path to ensure modules can be imported correctly.
from src.core.parser import HiveScriptParser
from src.jinja.environment import render_template
from src.paths import DATALAKE_SCRIPT_DIR, PROJECT_ROOT as HQL_SPARK_BRIDGE_ROOT

def _transform_and_export(script_path: Path, output_path: Path, transformer_class):
    """
    Internal helper to transform a single SQL script and export it as a Python script.

    This function encapsulates the core logic of parsing, transforming, and rendering.

    :param script_path: Path to the source SQL file.
    :param output_path: Path to the destination Python file.
    :param transformer_class: The transformer class (e.g., BasicPySparkTransformer) to use.
    """
    print(f"--- Processing: {script_path.name} ---")
    try:
        # Step 1: Parse the SQL file into a context object.
        print(f"  [1/4] Parsing SQL script...")
        context = HiveScriptParser.parse_file(str(script_path))
        print(f"      - Source: {context.source_name}, Table: {context.table_name}")

        # Step 2: Initialize the Transformer and convert the AST structure.
        print(f"  [2/4] Transforming AST...")
        transformer = transformer_class()
        render_model = transformer.transform(context)
        print(f"      - Transformation model created.")

        # Step 3: Render the data into the Jinja Template.
        print(f"  [3/4] Rendering PySpark script from template...")
        python_script = render_template(
            template_name="pyspark/pyspark_basic.jinja",
            render_model=render_model
        )

        # Step 4: Write the generated Python script to the output file.
        print(f"  [4/4] Exporting to: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(python_script, encoding='utf-8')
        print(f"--- SUCCESS: Finished {script_path.name} ---")
        return True

    except Exception as e:
        print(f"--- FAILED: Could not process {script_path.name} ---")
        print(f"    ERROR: {e}")
        return False

def batch_transform_scripts(transformer_class, input_dirs: list[Path], output_root_dir: Path):
    """
    Finds all SQL scripts in the specified input directories, transforms them
    using the given transformer class, and saves the output to a parallel
    directory structure.

    :param transformer_class: The transformer class to use for the conversion.
    :param input_dirs: A list of directories to scan for .sql files.
    :param output_root_dir: The root directory where the converted .py files will be saved.
    """
    print("=" * 60)
    print("STARTING BATCH TRANSFORMATION PROCESS")
    print(f"Transformer: {transformer_class.__name__}")
    print(f"Output Root: {output_root_dir}")
    print("=" * 60)

    total_files = 0
    processed_files = 0
    
    all_sql_files = []
    for input_dir in input_dirs:
        print(f"\nScanning directory: {input_dir}...")
        files_in_dir = sorted(list(input_dir.glob("**/*.sql")))
        if not files_in_dir:
            print("  No .sql files found in this directory.")
        else:
            print(f"  Found {len(files_in_dir)} .sql files.")
            all_sql_files.extend([(f, input_dir) for f in files_in_dir])

    total_files = len(all_sql_files)

    for script_path, base_dir in all_sql_files:
        # Determine the relative path to maintain the folder structure.
        relative_path = script_path.relative_to(base_dir)
        
        # Construct the output path, changing the extension to .py.
        # The output structure will be <output_root_dir>/<ddl|dml>/<com>/<filename>.py
        output_path = output_root_dir / base_dir.name / relative_path

        if _transform_and_export(script_path, output_path.with_suffix(".py"), transformer_class):
            processed_files += 1
    
    failed_files = total_files - processed_files
    print("\n" + "=" * 60)
    print("BATCH TRANSFORMATION COMPLETE")
    print(f"Summary: {total_files} total files found.")
    print(f"  - {processed_files} processed successfully.")
    print(f"  - {failed_files} failed.")
    print("=" * 60)

if __name__ == '__main__':
    # This section allows the script to be run directly for testing.
    # Example usage from project root: python -m src.utils.batch_processor

    from src.transformers.basic_pyspark_transformer import BasicPySparkTransformer

    # Define the directories to process based on the user's request.
    # dirs_to_process = [
    #     DATALAKE_SCRIPT_DIR / "dml" / "com",
    #     DATALAKE_SCRIPT_DIR / "ddl" / "com",
    # ]

    dirs_to_process = ['com', 'cur']

    for dir_path in dirs_to_process:

        # Define where the converted files will be stored.
        output_dir = HQL_SPARK_BRIDGE_ROOT / "samples" / "converted" / "dml"
        datalake_dir = DATALAKE_SCRIPT_DIR / "dml" / dir_path

        # Run the batch process.
        batch_transform_scripts(
            transformer_class=BasicPySparkTransformer,
            input_dirs=[datalake_dir],
            output_root_dir=output_dir
        )
