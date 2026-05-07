import re
from typing import Optional

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
            # [
            #     r"UNION\s+ALL",
            #     r"NOT\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+today_accounts",
            # ]
            # , [
            #     r"INSERT\s+INTO\s+TABLE.*?WHERE\s+NOT\s+EXISTS",
            #     r"record_updated_date|hash_value|updatets",
            # ]
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
