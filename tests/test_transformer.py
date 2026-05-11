from sqlglot import parse_one
import sqlglot

from src.transformers.utils import transform_outdated_com_raw_references

def test_transform():
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
  FROM ${com_schema}.r_mhbos_m_client AS mmc
  INNER JOIN filtered_clients AS fc
    ON mmc.client_no = fc.client_no
  WHERE
    mmc.part_id <= '${batch_date}'
)"""
    
    node = parse_one(sql, read="hive")
    transform_outdated_com_raw_references(node)
    print(node.sql(dialect="spark"))

if __name__ == "__main__":
    test_transform()
