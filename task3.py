from pyspark.sql.functions import split, explode, trim,col, year, to_date, count
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, countDistinct
import pandas as pd

df = pd.read_csv("updated_cleaned_dataset.csv")
# Calculate average popularity per explicit category
avg_popularity = df.groupby('Explicit')['Popularity'].mean()

print(avg_popularity)

# Spark Analysis for the avg popularity of (non)explicit songs per genre
spark = SparkSession.builder \
    .appName("ExplicitSongsPopularity") \
    .getOrCreate()
df = spark.read.csv("cleaned_spotify_dataset.csv", header=True, inferSchema=True)
df_split = df.withColumn("genre", explode(split("genre", ","))) \
             .withColumn("genre", trim("genre"))  # Remove extra spaces

genre_counts = df_split.groupBy("genre").agg(countDistinct("explicit").alias("explicit_variants"))
genres_with_both = genre_counts.filter("explicit_variants = 2").select("genre")

genre_popularity = df_split.groupBy("genre", "explicit") \
    .agg(round(avg("popularity"), 1).alias("avg_popularity")) \
    .join(genres_with_both, on="genre") \
    .orderBy("genre", "explicit")

genre_popularity.show(truncate=False)
genre_popularity.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("genre_popularity_explicit")

spark = SparkSession.builder.appName("YearlyExplicitAnalysis").getOrCreate()
df = spark.read.csv("cleaned_spotify_dataset.csv", header=True, inferSchema=True)

df = df.withColumn("release_date", to_date(col("Release Date"), "yyyy-MM-dd"))
df = df.withColumn("year", year(col("release_date")))

yearly_counts = df.groupBy("year", "Explicit").agg(count("*").alias("song_count")).orderBy("year", "Explicit")

yearly_counts.show()
yearly_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv("yearly_explicit_counts")

