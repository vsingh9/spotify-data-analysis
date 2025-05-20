from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, count
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName("YearlyExplicitAnalysis").getOrCreate()
df = spark.read.csv("cleaned_spotify_dataset.csv", header=True, inferSchema=True)

df = df.withColumn("release_date", to_date(col("Release Date"), "yyyy-MM-dd"))
df = df.withColumn("year", year(col("release_date")))

yearly_counts = df.groupBy("year", "Explicit").agg(count("*").alias("song_count")).orderBy("year", "Explicit")

yearly_counts.show()
yearly_counts.coalesce(1).write.mode("overwrite").option("header", "true").csv("yearly_explicit_counts")

# Convert to Pandas for visualization
pandas_df = yearly_counts.toPandas()

# Plot explicit vs non-explicit songs over the years
plt.figure(figsize=(14,7))
sns.lineplot(data=pandas_df, x="year", y="song_count", hue="Explicit", marker="o")
plt.title("Number of Explicit vs Non-Explicit Songs Released by Year")
plt.xlabel("Year")
plt.ylabel("Number of Songs")
plt.xticks(rotation=45)
plt.grid(True)
plt.show()
