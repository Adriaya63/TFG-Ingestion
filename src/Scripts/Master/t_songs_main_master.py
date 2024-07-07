from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def main():
    # Crear una sesión de Spark
    spark = SparkSession.builder \
        .appName("TransformSongsInfo") \
        .getOrCreate()
    
    # Leer el archivo CSV
    df = spark.read.format("avro").load("path/to/your/avrofile.avro")

    # Aplicar las transformaciones al DataFrame
    dfMain1 = df\
        .withColumn("cm_song_id",f.col("cr_isrc_id"))\
        .withColumn("cm_track_name",f.col("cr_track_name"))\
        .withColumn("cm_album_name",f.col("cr_album_name"))\
        .withColumn("cm_artist_name",f.col("cr_artist_name"))\
        .withColumn("cm_release_date", f.to_date(f.col("cr_release_date"), "yyyy-MM-dd"))\
        .withColumn("cm_gloval_rank", f.col("cr_gloval_rank"))\
        .withColumn("cm_track_score", f.col("cr_track_score"))\
        .withColumn("cm_explicit_content",f.col("cr_explicit_content"))\
        .select("cm_song_id","cm_track_name","cm_album_name","cm_artist_name","cm_release_date","cm_gloval_rank","cm_track_score","cm_explicit_content")
    
    dfMain2 = dfMain1\
        .withColumn("current_date", f.current_date())\
        .withColumn("cm_release_date_unix", f.unix_timestamp(f.col("cm_release_date")))\
        .withColumn("current_date_unix", f.unix_timestamp(f.col("current_date")))\
        .withColumn("cm_song_antiquity_unix",f.col("current_date_unix")-f.col("cm_release_date_unix"))

    dfMainRdo = dfMain2\
        .withColumn("closing_date",f.lit("2024-07-04"))\
        .withColumn("audit_date",f.current_timestamp())\
        .select("cm_song_id","cm_track_name","cm_album_name","cm_artist_name","cm_release_date","cm_gloval_rank","cm_track_score","cm_explicit_content","cm_song_antiquity_unix","closing_date","audit_date")

    
    # Guardar el DataFrame en formato Parquet particionado por el campo "closing_date"
    dfMainRdo.write.format("parquet").partitionBy("closing_date").save("path/to/output/directory")

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
