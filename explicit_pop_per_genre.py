from pyspark.sql.functions import split, explode, trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, countDistinct

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

