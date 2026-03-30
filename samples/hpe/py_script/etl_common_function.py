from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import subprocess
import re
import sys
import os

# for actual DAG schedule
#postgres_ctr_user = os.environ["POSTGRES_USER"]
#postgres_ctr_password = os.environ["POSTGRES_PASSWORD"]
#postgres_ctr_jdbc_url = f'jdbc:postgresql://{os.environ["POSTGRES_HOST"]}:{os.environ["POSTGRES_PORT"]}/{os.environ["POSTGRES_DB"]}'

# for execute spark-submit locally
postgres_ctr_user = "cable_usr"
postgres_ctr_password = "Welcome@123"
postgres_ctr_jdbc_url = f'jdbc:postgresql://KIBBKTDHEZAPP01.kenanga.local:5432/cable_db'

postgres_ctr_properties = {
    "user": postgres_ctr_user,
    "password": postgres_ctr_password,
    "driver": "org.postgresql.Driver"
}
    
def run_etl(source_name, table_name):
    spark = SparkSession.builder \
        .appName(f"ETL-{source_name}_{table_name}") \
        .config("spark.sql.hive.metastore.uris", "thrift://kibbktdhezapp01.kenanga.local:9083") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()
        
    query = f"""
            (SELECT 
            ext_start_time, 
            ext_end_time, 
            to_char(current_date, 'yyyyMMdd') AS today_date,
            to_char(current_date - interval '1 day', 'yyyyMMdd') AS yesterday_date,
            CASE
                WHEN coalesce(ext_end_time, to_char(current_date, 'yyyyMMdd')) <= to_char(current_date, 'yyyyMMdd') 
                  AND ext_start_time <= to_char(current_date, 'yyyyMMdd') 
                  AND coalesce(ext_end_time, to_char(current_date, 'yyyyMMdd')) >= ext_start_time
                  THEN 1
                ELSE 0
            END AS run_flag
            FROM public.c_etl_run
            WHERE source_name = '{source_name}' and table_name = '{table_name}'
            LIMIT 1) as subq"""
    
    df = spark.read.jdbc(
        url=postgres_ctr_jdbc_url,
        table=query,
        properties=postgres_ctr_properties
    )

    if df.count() == 0:
        raise ValueError("Query returned no rows")

    row = df.first()

    ext_start_time = row["ext_start_time"]
    ext_end_time = row["ext_end_time"]
    today_date = row["today_date"]
    yesterday_date = row["yesterday_date"]
    run_flag = row["run_flag"]

    print(f"ext_start_time     : {ext_start_time}")
    print(f"ext_end_time       : {ext_end_time}")
    print(f"Yesterday date     : {yesterday_date}")

    if not run_flag:
        print("Batch date is in the future. Exiting.")
        sys.exit(0)

    return spark, ext_start_time, ext_end_time, today_date, yesterday_date


def set_parameter(spark):
    query = "(SELECT var_name, var_value FROM public.c_p_all WHERE var_name <> '' and var_value <> '') as subq"

    df = spark.read.jdbc(
        url=postgres_ctr_jdbc_url,
        table=query,
        properties=postgres_ctr_properties
    )

    # Convert to dictionary
    params = {row["var_name"]: row["var_value"] for row in df.collect()}

    return params
    
def drop_partition_day(spark, batch_date, schema, tablename, partition_col, retention):
    batch_date_dt = datetime.strptime(batch_date, "%Y%m%d").date()
    cutoff_date_dt = batch_date_dt - timedelta(days=int(retention))
    cutoff_date_str = cutoff_date_dt.strftime("%Y%m%d")
    print(f"Batch date                  : {batch_date}")
    print(f"Retention                   : {retention} days")
    print(f"Cutoff date for housekeeping: {cutoff_date_str}")
	
    partitions_df = spark.sql(f"SHOW PARTITIONS {schema}.{tablename}")
    old_partitions = [
        row[0].split('=')[1]
        for row in partitions_df.collect()
        if row[0].split('=')[1] < cutoff_date_str
    ]

    for p in old_partitions:
        print(f"Dropping partition: {p}")
        spark.sql(f"ALTER TABLE {schema}.{tablename} DROP IF EXISTS PARTITION ({partition_col}='{p}')")
        
def get_available_files(base_path, start_date, end_date, file_pattern):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    file_list = []
    dates_with_files = []

    current = start
    while current <= end:
        d = current.strftime("%Y%m%d")
        
        filename = file_pattern.format(d)
        file_path = os.path.join(base_path, d, filename)

        if os.path.exists(file_path):
            file_list.append(file_path)
            dates_with_files.append(d)

        current += timedelta(days=1)

    if not dates_with_files:
        return None, None, []

    min_date = min(dates_with_files)
    max_date = max(dates_with_files)

    return min_date, max_date, file_list
    
