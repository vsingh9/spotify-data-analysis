import time
from pyspark.sql.functions import split, explode, trim, col, year, to_date, count, avg, round, countDistinct
from pyspark.sql import SparkSession
import pandas as pd
import logging
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

# Task 3 start
def get_spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
# Start overall timer
start_time = time.time()

# ---------- Pandas Analysis ----------
df = pd.read_csv("updated_cleaned_dataset.csv")
avg_popularity = df.groupby('Explicit')['Popularity'].mean()
print(avg_popularity)

# ---------- Spark Genre Popularity Analysis ----------
spark = get_spark_session("ExplicitSongsPopularity")
df = spark.read.csv("cleaned_spotify_dataset.csv", header=True, inferSchema=True)
df_split = df.withColumn("genre", explode(split("genre", ","))) \
             .withColumn("genre", trim("genre"))

genre_counts = df_split.groupBy("genre").agg(countDistinct("explicit").alias("explicit_variants"))
genres_with_both = genre_counts.filter("explicit_variants = 2").select("genre")

genre_popularity = df_split.groupBy("genre", "explicit") \
    .agg(round(avg("popularity"), 1).alias("avg_popularity")) \
    .join(genres_with_both, on="genre") \
    .orderBy("genre", "explicit")

genre_popularity.show(truncate=False)
genre_popularity.coalesce(1).write.mode("overwrite").option("header", "true").csv("genre_popularity_explicit")

# ---------- Spark Yearly Explicit Analysis ----------
spark = get_spark_session("YearlyExplicitAnalysis")

df = spark.read.csv("cleaned_spotify_dataset.csv", header=True, inferSchema=True)
df = df.withColumn("release_date", to_date(col("Release Date"), "yyyy-MM-dd"))
df = df.withColumn("year", year(col("release_date")))

yearly_counts = df.groupBy("year", "Explicit").agg(count("*").alias("song_count")).orderBy("year", "Explicit")
yearly_counts.show()
yearly_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv("yearly_explicit_counts")

# ---------- Total Time ----------
print(f"\nTotal script runtime: {time.time() - start_time:.2f} seconds")
# Task 3 end
