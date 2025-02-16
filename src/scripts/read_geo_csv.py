import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("read_geo_csv") \
    .master("yarn") \
    .config("spark.deploy.mode", "cluster") \
    .getOrCreate()


import datetime
import pyspark.sql.functions as F

try:
    # Читаем CSV файл из HDFS
    geo_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ";") \
        .csv("/user/malina692/data/geo/geo.csv")

    # Выводим схему DataFrame
    geo_df.printSchema()

    # Выводим первые несколько строк DataFrame
    geo_df.show()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    spark.stop()