from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CompareAvroAndCSV") \
    .getOrCreate()

#Lectura del archivo Parquet de Master
parquet_file_path = ""
df_parquet = spark.read.parquet(parquet_file_path)
master_count = df_parquet.count()

# Lectura del archivo Avro de Raw
avro_file_path = "ruta/a/la/tabla.avro"
df_avro = spark.read.format("avro").load(avro_file_path)
raw_count = df_avro.count()


# Comprobar que la cantidad de registros es la misma
if master_count == raw_count:
    print(f"La cantidad de registros es la misma: {master_count}")
else:
    print(f"La cantidad de registros es diferente. Master: {master_count}, Avro: {raw_count} ")

# Detener la sesión de Spark
spark.stop()
