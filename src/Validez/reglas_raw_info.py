from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CompareAvroAndCSV") \
    .getOrCreate()

# Lectura del archivo Avro de Raw
avro_file_path = "ruta/a/la/tabla.avro"
df_avro = spark.read.format("avro").load(avro_file_path)
avro_count = df_avro.count()

#Lectura del archivo CSV de Staging
csv_file_path = "ruta/al/archivo.csv"
df_csv = spark.read.option("delimiter", ",").option("header", True).csv("csv_file_path")
csv_count = df_csv.count()

# Comprobar que la cantidad de registros es la misma
if avro_count == csv_count:
    print(f"La cantidad de registros es la misma: {avro_count}")
else:
    print(f"La cantidad de registros es diferente. Avro: {avro_count}, CSV: {csv_count}")

# Detener la sesión de Spark
spark.stop()
