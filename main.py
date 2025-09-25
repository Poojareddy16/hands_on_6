# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets 
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)
# Join logs with songs metadata
logs_songs = logs.join(songs, on="song_id", how="inner")


# Task 1: User Favorite Genres 
genre_count = logs_songs.groupBy("user_id", "genre").count()
window_user = Window.partitionBy("user_id").orderBy(desc("count"))

favorite_genre = (
    genre_count.withColumn("rank", row_number().over(window_user))
    .filter(col("rank") == 1)
    .drop("rank")
)

favorite_genre.write.mode("overwrite").csv("output/user_favorite_genres/")


# Task 2: Average Listen Time 
avg_listen_time = logs.groupBy("song_id").agg(
    avg("duration_sec").alias("avg_duration")
)
avg_listen_time = avg_listen_time.join(
    songs.select("song_id", "title"), on="song_id"
)

avg_listen_time.write.mode("overwrite").csv("output/avg_listen_time_per_song/")



# Task 3: Genre Loyalty Scores 
# Task 3: Genre Loyalty Scores

# Total plays per user
total_plays = logs_songs.groupBy("user_id").count().withColumnRenamed("count", "total")

# Plays per user per genre
user_genre_counts = logs_songs.groupBy("user_id", "genre").count().withColumnRenamed("count", "genre_count")

# Join with favorite genre to get top genre plays per user
top_genre = user_genre_counts.join(
    favorite_genre.select("user_id", "genre"), 
    on=["user_id", "genre"], 
    how="inner"
).withColumnRenamed("genre_count", "top_genre_count")

# Calculate loyalty score
loyalty = top_genre.join(total_plays, on="user_id").withColumn(
    "loyalty_score", col("top_genre_count") / col("total")
).filter(col("loyalty_score") > 0.4)

loyalty.write.mode("overwrite").csv("output/genre_loyalty_scores/")



# Task 4: Identify users who listen between 12 AM and 5 AM
night_logs = logs.withColumn("hour", hour(to_timestamp("timestamp")))
night_users = (
    night_logs.filter((col("hour") >= 0) & (col("hour") < 5))
    .select("user_id")
    .distinct()
)

night_users.write.mode("overwrite").csv("output/night_owl_users/")
spark.stop()