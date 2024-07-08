from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("CSVValidationAndGroupBy") \
    .getOrCreate()

# Ruta al archivo CSV
csv_file_path = "ruta/al/archivo.csv"

# Leer el archivo CSV
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Regla 1: Verificar que el archivo no esté vacío
if df.rdd.isEmpty():
    raise ValueError("El archivo CSV está vacío.")

# Regla 2: Verificar que no haya registros nulos en los campos de la clave primaria
primary_key_column = "ISRC"
df_non_null_pk = df.filter(F.col(primary_key_column).isNotNull())

if df.count() != df_non_null_pk.count():
    raise ValueError(f"Existen registros nulos en el campo {primary_key_column}.")

# Regla 3: Verificar que no haya duplicados en los campos de la clave primaria
df_unique_pk = df_non_null_pk.dropDuplicates([primary_key_column])

if df_non_null_pk.count() != df_unique_pk.count():
    raise ValueError(f"Existen registros duplicados en el campo {primary_key_column}.")

# Agrupar por el campo de la clave primaria y contar las apariciones, luego ordenar de forma descendente
df_grouped = df_unique_pk.groupBy(primary_key_column).count().orderBy(F.col("count").desc())

# Mostrar el DataFrame resultante
df_grouped.show()

# Detener la sesión de Spark
spark.stop()
