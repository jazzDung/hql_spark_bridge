# A central place to define all constants for paths
import os
from pathlib import Path
import yaml

# Define the project root directory.
# This is determined by going up two levels from the current file's location (src/paths.py -> src -> project_root).
# Using .resolve() makes it an absolute path, ensuring it works regardless of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# --- Configuration Paths ---
# Define the path to the main configuration directory.
CONFIGS_DIR = PROJECT_ROOT / "configs"

# Define specific configuration file paths for easy access
DIRECTORY_CONFIG_PATH = CONFIGS_DIR / "directory" / "directory.yaml"
VARIABLE_CONFIG_PATH = CONFIGS_DIR / "rules" / "variable.yaml"
EXT_VARIABLE_CONFIG_PATH = CONFIGS_DIR / "rules" / "ext_variable.yaml"

# RULES_GENERAL_CONFIG = CONFIGS_DIR / "rules" / "optimizations" / "general.yaml"


# 2. Load Datalake repo path configuration from YAML
with open(DIRECTORY_CONFIG_PATH, 'r', encoding='utf-8') as f:
    directory_paths = yaml.safe_load(f)

DATALAKE_SCRIPT_DIR = Path(directory_paths['datalake_root'])
EXT_CONFIG_PATH = DATALAKE_SCRIPT_DIR / "etc" / "ext_config.conf"

# --- Template Paths ---
# Define the path to the Jinja templates directory
TEMPLATES_DIR = PROJECT_ROOT / "template"

# --- External Resource Paths ---
# Path to the directory containing external XML definitions.
INCREMENATL_EXT_DIR = DATALAKE_SCRIPT_DIR / "ext" / "xml"
INITIAL_EXT_DIR = DATALAKE_SCRIPT_DIR / "ext" / "xml"

# --- Other Important Paths ---
# Add other project-related paths here as needed.
# For example:
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
# DATA_DIR = PROJECT_ROOT / "data"
# OUTPUT_DIR = PROJECT_ROOT / "output"


# This utility function can be run directly to verify that all critical paths are resolved correctly.
def check_paths():
    """
    Checks if the defined paths exist and prints their status.
    This is useful for debugging path-related issues.
    """
    paths_to_check = {
        "PROJECT_ROOT": PROJECT_ROOT,
        "CONFIGS_DIR": CONFIGS_DIR,
        "DIRECTORY_CONFIG": DIRECTORY_CONFIG_PATH,
        # "RULES_GENERAL_CONFIG": RULES_GENERAL_CONFIG,
        "TEMPLATES_DIR": TEMPLATES_DIR,
        "DATALAKE_SCRIPT_DIR": DATALAKE_SCRIPT_DIR,
        "EXTERNAL_XML_DIR": INCREMENATL_EXT_DIR,
    }
    print("--- Verifying critical project paths ---")
    for name, path in paths_to_check.items():
        status = "Exists" if path.exists() else "Not Found"
        print(f"{name}: {path} - Status: {status}")
    print("--- Verification complete ---")

# This allows you to run 'python -m src.paths' from the project root to check everything.
if __name__ == "__main__":
    check_paths()
