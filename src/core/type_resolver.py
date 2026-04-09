import yaml
from pathlib import Path


class TypeResolver:
    _config = None
    _config_path = None

    @classmethod
    def _get_config_path(cls) -> Path:
        if cls._config_path is None:
            # src/core/type_resolver.py -> src/core -> src -> project_root
            project_root = Path(__file__).resolve().parents[2]
            cls._config_path = project_root / "configs" / "rules" / "data_type.yaml"
        return cls._config_path

    @classmethod
    def _load_config(cls):
        if cls._config is None:
            config_path = cls._get_config_path()
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")

            with config_path.open("r", encoding="utf-8") as f:
                cls._config = yaml.safe_load(f)

        return cls._config

    @classmethod
    def convert_datatype(cls, data_type: str, input_dialect: str = 'mssql', output_dialect: str = 'canonical_types') -> str:
        conf = cls._load_config()
        # 1. Find Canonical ID from Dialect
        canonical_id = conf['dialect_mapping'].get(input_dialect, {}).get(data_type.lower(), "STRING")

        # 2. Find Hive Type from Canonical ID
        if output_dialect == 'canonical_types':
            return canonical_id

        for t in conf['canonical_types']:
            if t['id'] == canonical_id:
                return t[output_dialect]

        return "STRING"

    @classmethod
    def get_datatype(cls, canonical_id: str, dialect: str = 'hive') -> str:
        conf = cls._load_config()

        # 2. Find Hive Type from Canonical ID
        for t in conf['canonical_types']:
            if t['id'] == canonical_id:
                return t[dialect]

        return "STRING"
