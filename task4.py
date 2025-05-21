# task 4 - what emotional tones are associated with songs that are considered "good for exercise" or "good for study"?
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import FloatType
from distutils.version import LooseVersion

# imports to perform analysis, plotting, and track runtime
import pandas as pd
import matplotlib.pyplot as plt
import time

start_time_task4 = time.time()

spark = SparkSession.builder \
        .appName("Spotify Analysis Task 4") \
        .getOrCreate()

df = spark.read.csv("updated_cleaned_dataset.csv", header = True, inferSchema = True)

# df.show(1, truncate=False)

# perform further data cleaning to handle field overflows and data types
clean_df = df.filter((col("emotion") == "joy") | (col("emotion") == "love") | (col("emotion") == "anger") | (col("emotion") == "fear") | (col("emotion") == "surprised") | (col("emotion") == "sadness"))
clean_df = clean_df.withColumn('Tempo', clean_df['Tempo'].cast(FloatType()))

# use filter transformation to find songs good for running, studying, or relaxing
run_songs = clean_df.filter(col("Good for Running") == 1)
study_songs = clean_df.filter(col("Good for Work/Study") == 1)
relax_songs = clean_df.filter(col("Good for Relaxation/Meditation") == 1)

# calculate emotion distributions
run_emotions = run_songs.groupBy("emotion").count()
study_emotions = study_songs.groupBy("emotion").count()
relax_emotions = relax_songs.groupBy("emotion").count()

# convert final emotion_counts dataframes into csv for db purposes
# run_emotions.write.csv("run_emotion_counts.csv", header=True, mode='overwrite')
# study_emotions.write.csv("study_emotion_counts.csv", header=True, mode='overwrite')
# relax_emotions.write.csv("relax_emotion_counts.csv", header=True, mode='overwrite')

# convert dataframe to pandas for analysis purposes
run_pd = run_emotions.toPandas()
study_pd = study_emotions.toPandas()
relax_pd = relax_emotions.toPandas()

# visualize emotion distributions using a bar graph
plt.bar(run_pd['emotion'], run_pd['count'])
plt.title('Emotion Distribution of Running Songs')
plt.xlabel('Emotion')
plt.ylabel('Number of Songs')
plt.show()

plt.bar(study_pd['emotion'], study_pd['count'])
plt.title('Emotion Distribution of Study Songs')
plt.xlabel('Emotion')
plt.ylabel('Number of Songs')
plt.show()

plt.bar(relax_pd['emotion'], relax_pd['count'])
plt.title('Emotion Distribution of Relaxation Songs')
plt.xlabel('Emotion')
plt.ylabel('Number of Songs')
plt.show()

# perform extra analysis on average tempo for each "good for ..." category
run_tempo = run_songs.groupBy("Good for Running").avg("Tempo").show()
study_tempo = study_songs.groupBy("Good for Work/Study").avg("Tempo").show()
relax_tempo = relax_songs.groupBy("Good for Relaxation/Meditation").avg("Tempo").show()

# convert final tempo_avg dataframes into csv for db purposes
# run_tempo.write.csv("run_tempo_avg.csv", header=True, mode='overwrite')
# study_tempo.write.csv("study_tempo_avg.csv", header=True, mode='overwrite')
# relax_tempo.write.csv("relax_tempo_avg.csv", header=True, mode='overwrite')

print(f"\nTask 4 Runtime: {time.time() - start_time_task4:.2f} seconds")