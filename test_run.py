import subprocess
import sys
from sqlglot import parse_one, exp
from src.transformers.utils import refactor_ast_for_com_layer
from src.context.sql_conversion_context import SqlConversionContext

sql = """WITH filtered_clients AS (
  SELECT DISTINCT
    client_no
  FROM ${com_schema}.r_mhbos_m_client
  WHERE
    part_id = '${batch_date}'
  UNION
  SELECT DISTINCT
    client_no
  FROM ${com_schema}.r_mhbos_m_client_ext
  WHERE
    part_id = '${batch_date}'
), latest_clients AS (
  SELECT
    mmc.*,
    ROW_NUMBER() OVER (PARTITION BY mmc.client_no ORDER BY mmc.part_id DESC) AS rn
  FROM ${raw_schema}.r_mhbos_m_client AS mmc
  INNER JOIN filtered_clients AS fc
    ON mmc.client_no = fc.client_no
  WHERE
    mmc.part_id <= '${batch_date}'
)"""

node = parse_one(sql, read="hive")

context = SqlConversionContext(
    original_file_path="",
    raw_sql_content="",
    source_name="",
    table_name="",
    layer="com",
    sub_layer="m"
)
refactor_ast_for_com_layer(node, context)
print(node.sql(dialect="spark", pretty=True))
