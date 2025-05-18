# task 1 - what is the distribution of emotions across different music genres?
from pyspark.sql import SparkSession, functions

import matplotlib.pyplot as plt
import seaborn as sns

# Create Spark Session
spark = SparkSession.builder \
        .appName("Analyze Emotions and Genres") \
        .getOrCreate()

# Read CSV datafile
df = spark.read.csv("updated_cleaned_dataset.csv", header=True, inferSchema=True)

df = df.withColumn("Genres_Array", functions.split(df["Genre"], ","))
# df = df.withColumn("Emotions_Array", functions.split(df["emotion"], ","))

# df.show(50)

df = df.withColumn("Genre", functions.explode(df["Genres_Array"]))
# df = df.withColumn("Emotion", functions.explode(df["Emotions_Array"]))

# df.show(100)

# df_grouped_genre_songs = df.groupBy("Genre").agg(
#       functions.collect_list("Emotion").alias("Emotion")
# )

genre_emotion_counts = df.groupBy("Genre", "emotion").count().orderBy("Genre", "coun>
genre_emotion_counts.show()

# df_grouped_genre_songs.show(50)

# print(df_grouped_genre_songs.count())

pdf = genre_emotion_counts.toPandas()

pivot_df = pdf.pivot(index="Genre", columns="emotion", values="count").fillna(0)

plt.figure(figsize=(16, len(pivot_df) * 0.4))
sns.heatmap(pivot_df, annot=True, fmt=".0f", cmap="coolwarm", linewidths=.5)

plt.title("Distribution of Emotions Across Different Genres")
plt.ylabel("Genre")
plt.xlabel("Emotion")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
