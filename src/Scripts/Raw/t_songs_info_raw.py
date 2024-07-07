from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def main():
    # Crear una sesión de Spark
    spark = SparkSession.builder \
        .appName("TransformSongsInfo") \
        .getOrCreate()
    
    # Leer el archivo CSV
    df = spark.read.option("delimiter", ",").option("header", True).csv("StreamSpoty.csv")    
    
    # Aplicar las transformaciones al DataFrame
    dfRaw = df\
        .withColumn("cr_track_name", f.col("Track"))\
        .withColumn("cr_album_name", f.col("Album Name"))\
        .withColumn("cr_artist_name", f.col("Artist"))\
        .withColumn("cr_release_date", f.date_format(f.to_date(f.col("Release Date"), "M/d/yyyy"), "yyyy-MM-dd"))\
        .withColumn("cr_isrc_id", f.col("ISRC"))\
        .withColumn("cr_gloval_rank", f.col("All Time Rank"))\
        .withColumn("cr_track_score", f.col("Track Score"))\
        .withColumn("cr_spotify_sreams", f.col("Spotify Streams"))\
        .withColumn("cr_spotify_playlist_n", f.col("Spotify Playlist Count"))\
        .withColumn("cr_spotify_playlist_r", f.col("Spotify Playlist Reach"))\
        .withColumn("cr_spotify_popularity", f.col("Spotify Popularity"))\
        .withColumn("cr_youtube_views", f.col("YouTube Views"))\
        .withColumn("cr_youtube_likes", f.col("YouTube Likes"))\
        .withColumn("cr_tiktok_posts", f.col("TikTok Posts"))\
        .withColumn("cr_tiktok_likes", f.col("TikTok Likes"))\
        .withColumn("cr_tiktok_views", f.col("TikTok Views"))\
        .withColumn("cr_youtube_playlist_r", f.col("YouTube Playlist Reach"))\
        .withColumn("cr_applemusic_playlist_c", f.col("Apple Music Playlist Count"))\
        .withColumn("cr_airplay_streams", f.col("AirPlay Spins"))\
        .withColumn("cr_siriusxm_streams", f.col("SiriusXM Spins"))\
        .withColumn("cr_deezer_playlist_c", f.col("Deezer Playlist Count"))\
        .withColumn("cr_deezer_playlist_r", f.col("Deezer Playlist Reach"))\
        .withColumn("cr_amazon_playlist_c", f.col("Amazon Playlist Count"))\
        .withColumn("cr_pandora_streams", f.col("Pandora Streams"))\
        .withColumn("cr_pandora_posts", f.col("Pandora Track Stations"))\
        .withColumn("cr_soundcloud_streams", f.col("Soundcloud Streams"))\
        .withColumn("cr_shazam_counts", f.col("Shazam Counts"))\
        .withColumn("cr_tidal_popularity", f.col("TIDAL Popularity"))\
        .withColumn("cr_explicit_content", f.col("Explicit Track"))\
        .withColumn("closing_date", f.lit("2024-07-04"))\
        .withColumn("audit_date", f.current_timestamp())\
        .select("cr_track_name", "cr_album_name", "cr_artist_name", "cr_release_date", "cr_isrc_id",
                "cr_gloval_rank", "cr_track_score", "cr_spotify_sreams",
                "cr_spotify_playlist_n", "cr_spotify_playlist_r",
                "cr_spotify_popularity", "cr_youtube_views", "cr_youtube_likes",
                "cr_tiktok_posts", "cr_tiktok_likes", "cr_tiktok_views", "cr_youtube_playlist_r",
                "cr_applemusic_playlist_c", "cr_airplay_streams", "cr_siriusxm_streams",
                "cr_deezer_playlist_c", "cr_deezer_playlist_r", "cr_amazon_playlist_c",
                "cr_pandora_streams", "cr_pandora_posts", "cr_soundcloud_streams",
                "cr_shazam_counts", "cr_tidal_popularity", "cr_explicit_content", "closing_date", "audit_date")
    
    # Guardar el DataFrame en formato Avro particionado por el campo "closing_date"
    dfRaw.write.format("avro").partitionBy("closing_date").save("path/to/output/directory")

    # Detener la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
