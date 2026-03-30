from pyspark.sql import SparkSession
import os
import sys

mssql_jar = os.path.abspath(os.path.join(os.getcwd(), "../deploy/jars", "mssql-jdbc-12.2.0.jre8.jar"))
pg_jar = os.path.abspath(os.path.join(os.getcwd(), "../deploy/jars", "postgresql-42.6.0.jar"))
extra_jars = [str(p) for p in [mssql_jar, pg_jar] if p is not None]


def get_spark_bridge_session():
    # Giả sử bạn để thư mục hadoop-bin trong project
    os.environ['HADOOP_HOME'] = os.path.abspath("../deploy/hadoop-bin")
    os.environ['hadoop.home.dir'] = os.environ['HADOOP_HOME']
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    # Thêm đường dẫn bin vào PATH của script hiện tại
    sys.path.append(os.path.join(os.environ['HADOOP_HOME'], "bin"))

    # Configure necessary JARs (MSSQL & Postgres)
    # Note: You need to download these 2 jar files into the ./jars directory in the project
    spark = SparkSession.builder \
        .appName("hql-spark-bridge-engine") \
        .master("spark://localhost:7077") \
        .config("spark.driver.host", "host.docker.internal") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .enableHiveSupport() \

    if extra_jars:
        spark = spark.config("spark.jars", ",".join(extra_jars))

    spark = spark.getOrCreate()

    return spark



# .config("spark.jars", "./jars/mssql-jdbc-12.2.0.jrers/postgresql-42.6.0.jar") \
# .config("spark.driver.extraClassPath", "./jars/mssql-jdbc-12.2.8.jar,./ja0.jre8.jar:./jars/postgresql-42.6.0.jar") \


if __name__ == "__main__":
    session = get_spark_bridge_session()
    print("--- Spark Bridge Session Created Successfully ---")

    # Test Hive Metastore connection
    # session.sql("SHOW DATABASES").show()
    session.range(1, 10).show()

    # Test MSSQL connection (Data Source)
    # session.read.format("jdbc").options(...).load()