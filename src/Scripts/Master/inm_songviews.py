import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

def main(fecha_ejec):
    # Crear una sesión de Spark
    spark = SparkSession.builder \
        .appName("TransformSongsInfo") \
        .getOrCreate()
    
#Rutas de entrada y salida
    input_path = "src/Data/StreamSpoty.csv"
    output_path = "src/Data/Raw/t_songs_info_raw"

    # Leer el archivo CSV
    df_input = spark.read.format("avro").load(input_path)

    #Filtrado por la fecha de particion
    filtro = "closing date == '"+fecha_ejec+"'" 
    df = df_input.filter(fecha_ejec)

    # Expresion lamda para castear a IntegerType
    clean_and_convert_to_int = f.udf(lambda x: int(x.replace(",", "")) if x and x.strip() else 0, IntegerType())


    # Aplicar las transformaciones al DataFrame
    dfViews1 = df\
        .withColumn("cm_song_id",f.col("cr_isrc_id"))\
        .withColumn("cm_gloval_rank", f.col("cr_gloval_rank"))\
        .withColumn("cm_spotify_sreams",clean_and_convert_to_int(f.col("cr_spotify_sreams")))\
        .withColumn("cm_youtube_views",clean_and_convert_to_int(f.col("cr_youtube_views")))\
        .withColumn("cm_shazam_counts",clean_and_convert_to_int(f.col("cr_shazam_counts")))\
        .withColumn("cm_soundcloud_streams",clean_and_convert_to_int(f.col("cr_soundcloud_streams")))\
        .select("cm_song_id","cm_gloval_rank","cm_spotify_sreams","cm_youtube_views","cm_shazam_counts","cm_soundcloud_streams")
    
    dfViewsRdo = dfViews1\
        .withColumn("cm_total_views",f.col("cm_spotify_sreams")+f.col("cm_youtube_views")+f.col("cm_shazam_counts")+f.col("cm_soundcloud_streams"))\
        .withColumn("closing_date",f.lit(fecha_ejec))\
        .withColumn("audit_date",f.current_timestamp())\
        .select("cm_song_id","cm_gloval_rank","cm_total_views","cm_spotify_sreams","cm_youtube_views",\
                "cm_shazam_counts","cm_soundcloud_streams","closing_date","audit_date")
    
    # Guardar el DataFrame en formato Parquet particionado por el campo "closing_date"
    dfViewsRdo.write.format("parquet").partitionBy("closing_date").save(output_path)

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: t_songs_info_raw.py <fecha_ejec>")
        sys.exit(1)
    
    fecha_ejec = sys.argv[1]
    main(fecha_ejec)
