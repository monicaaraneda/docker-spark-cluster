from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, desc, lit, struct, avg,sum,date_format,count,when
from functools import reduce 
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import round
import pyspark.sql.functions as func

  
def init_spark():
  sql = SparkSession.builder\
    .appName("trip-app")\
    .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def main():
  url = "jdbc:postgresql://demo-database:5432/mta_data"
  properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
  }
  
  file = "/opt/spark-data/HLTau.csv"
  sql,sc = init_spark()

  df = sql.read.load(file,format = "csv", inferSchema="true", sep=",", header="true"
      ) \
      .withColumn("u2", col("u")**2) \
      .withColumn("v2", col("v")**2) \
      .withColumn("square", col("u2")+col("v2")) \
      .withColumn("raiz", col("square")**(1/2)) \
      .withColumn("value", when((col('raiz') < 3000 ) | (col('raiz') > 3000), None).otherwise(col('raiz'))) \
      .withColumn("visibilidad", func.round(col("raiz"), 2))
  
  # Filter invalid coordinates
  df.show()

  # Count & groups
  df.select(avg(col('visibilidad'))).show()
  df.distinct().show()
  N = df.count()
   
  df_1_new = df.filter(col('visibilidad') > 3111) \
    .agg(count(func.lit(1)).alias("Num Of Records"),func.mean("r").alias("Media real"),func.mean("i").alias("Media imaginaria"),func.mean("raiz").alias("Potencia"),func.mean("w").alias("Ruido total"))
  

  df_2_new = df.filter(col('visibilidad') < 3111) \
    .agg(count(func.lit(1)).alias("Num Of Records"),func.mean("r").alias("Media real"),func.mean("i").alias("Media imaginaria"),func.mean("raiz").alias("Potencia"),func.mean("w").alias("Ruido total"))


  result = df_1_new.union(df_2_new)

  result.show()

  #partitionBy() control number of partitions
  result.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("visibilidad") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")
  
if __name__ == '__main__':
  main()
