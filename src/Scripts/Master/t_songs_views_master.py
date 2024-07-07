import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Crear una sesión de Spark
    spark = SparkSession.builder \
        .appName("TransformSongsInfo") \
        .getOrCreate()
    
    # Leer el archivo CSV
    df = spark.read.format("avro").load("path/to/your/avrofile.avro")

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
        .withColumn("closing_date",f.lit("2024-07-04"))\
        .withColumn("audit_date",f.current_timestamp())\
        .select("cm_song_id","cm_gloval_rank","cm_total_views","cm_spotify_sreams","cm_youtube_views",\
                "cm_shazam_counts","cm_soundcloud_streams","closing_date","audit_date")
    
    # Guardar el DataFrame en formato Parquet particionado por el campo "closing_date"
    dfViewsRdo.write.format("parquet").partitionBy("closing_date").save("path/to/output/directory")

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
