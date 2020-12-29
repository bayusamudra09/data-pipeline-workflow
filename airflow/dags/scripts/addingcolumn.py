from os.path import abspath, exists

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Forex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


today = datetime.date.today()
yesterday = today - datetime.timedelta(days = 1)

#use your gcp internal_ip and hdfs_port as default 8020
df_today = spark.read.json(f'hdfs://internal_ip:hdfs_port~/covid/covid_{str(today)}.json')

if exists(f'~/airflow/dags/files/covid_{str(yesterday)}.json'):
    df_yesterday = spark.read.json(f'hdfs://internal_ip:hdfs_port~/covid/covid_{str(yesterday)}.json')
    update_covid = df_today.select(col('tanggal'),col("provinsi"),col("positif"),col("sembuh"),col("meninggal"))\
    .join(df_yesterday.select(col("positif").alias("positif_2"),col("sembuh").alias("sembuh_2"),col("meninggal").alias("meninggal_2"),col("provinsi")), "provinsi")\
    .withColumn("pertambahan_positif", (col("positif")-col("positif_2")))\
    .withColumn("pertambahan_sembuh", (col("sembuh")-col("sembuh_2")))\
    .withColumn("pertambahan_meninggal", (col("meninggal")-col("meninggal_2")))\
    .drop("positif_2", "sembuh_2","meninggal_2")

else:
	update_covid = df_today.withColumn("pertambahan_positif", col("positif")).withColumn("pertambahan_sembuh", col("sembuh")).withColumn("pertambahan_meninggal", col("meninggal"))


update_covid = update_covid.select("tanggal","provinsi", "positif", "sembuh", "meninggal", "pertambahan_positif","pertambahan_sembuh", "pertambahan_meninggal")
update_covid.write.mode("append").insertInto("covid_update_dataset")
