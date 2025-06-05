import time
from pyspark.sql.functions import split, explode, trim, col, year, to_date, count, avg, round, countDistinct, corr
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from distutils.version import LooseVersion
import pandas as pd
import logging
import matplotlib.pyplot as plt
import seaborn as sns
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

# ---------------------------------------------------------------- TASK 1 ----------------------------------------------------------------
# task 1 - what is the distribution of emotions across different music genres?
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

# ---------------------------------------------------------------- TASK 2 ----------------------------------------------------------------
# task 2 - how has the popularity of different music genres changed over time?
spark = SparkSession.builder \
    .appName("GenrePopularityOverTime") \
    .getOrCreate()
 
# start overall timer 
start_time_task2 = time.time()

# remove all the other message beside the output 
spark.sparkContext.setLogLevel("WARN")

# uploading the dataset
df = spark.read.csv("/Users/emily/Desktop/cleaned_spotify_dataset_1.csv/updated_cleaned_dataset.csv",
    header = True,
    inferSchema = True)

#to found out the distinct genre type (explode each comma-separed genre)
one_genre = df.select(
	"*",
	F.explode(F.split(F.col("Genre"), ",")).alias("single_genre")
	).withColumn("single_genre", F.trim(F.col("single_genre"))) # remove white spaces

distinct_genres = one_genre.select("single_genre").distinct()
count = distinct_genres.count()
distinct_genres.show(count,truncate=False)
print(f"There are total of {count} different genres")
print() #newline

# add a new column call "year" of the Release Date
df = df.withColumn("year",F.year("Release Date"))

# Split comma-separated Genre strings and explode into one row per genre
genres = df.select(
    F.explode(F.split(F.col("Genre"), ",")).alias("genre"),
    F.col("Popularity").cast("double").alias("popularity")
).withColumn("genre", F.trim(F.col("genre")))

# Compute average popularity and count per genre
genre_stats = genres.groupBy("genre").agg(
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.count("*").alias("track_count")
)

# filter out genres with very few tracks, e.g. fewer than 5
genre_stats = genre_stats.filter(F.col("track_count") >= 5)

# Order by descending average popularity and take top 10
top10 = genre_stats.orderBy(F.desc("avg_popularity")).limit(10)

# Show the results
print("\nTop 10 genres by average popularity:\n")
top10.show(truncate=False)

# convert to pandas for visulization 
plot1 = top10.toPandas().set_index("genre")
# Plot a bar chart
plt.figure()
plot1['avg_popularity'].plot(kind='bar')
plt.xlabel('Genre')
plt.ylabel('Average Popularity')
plt.title('Top 10 Genres by Average Popularity')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

#find out the average popularity for each year and rank the genre for the top 10 highest avg_popularity
genre_by_year = df.select(
	"year",
	F.explode(F.split(F.col("Genre"),",")).alias("genre"),
	F.col("Popularity").cast("double").alias("popularity")
).withColumn("genre", F.trim(F.col("genre")))

year_genre_stats = (
    genre_by_year
        .groupBy("year", "Genre")
        .agg(
            F.round(F.avg("Popularity"), 2).alias("avg_popularity"),
            F.count("*").alias("track_count")
             )
    )

# filter out group that don't have much data (less than 5 tracks)
filtered_stats = year_genre_stats.filter(F.col("track_count") >= 5)

# rank the average popularity for each year 
w = Window.partitionBy("year").orderBy(F.desc("avg_popularity"))
ranked = filtered_stats.withColumn("rank", F.row_number().over(w))

#filter the top 5 most-common genres for each year 
top5_genres = ranked.filter(F.col("rank") <= 5)

table = top5_genres.select("year",
            "Genre",
            "avg_popularity",
            "track_count",
            "rank"
    )

# displaying the table 
table.orderBy("year", "rank").show(100, truncate=False)

#turn the top10 genre dataframe into a python list
top10_list = [row["genre"]for row in top10.select("genre").collect()]
yearly_top10=filtered_stats.filter(F.col("genre").isin(top10_list))

print("\nThe peak year for the top 10 genres have the highest average popularity:")
peaks_for_genre = (
    	yearly_top10
      		.groupBy("genre")
      		.agg(
       		   F.max(
         	   F.struct(
           	   F.col("avg_popularity"), 
           	   F.col("year")
          )
        ).alias("maxrec")
      )
      .select(
        "genre",
        F.col("maxrec.year").alias("peak_year"),
        F.col("maxrec.avg_popularity").alias("peak_popularity")
      )
   )
peaks_for_genre.show(truncate=False)

# bar plox
plot2 = peaks_for_genre.toPandas().set_index("genre")
plt.figure()
ax = plot2["peak_popularity"].plot(kind="bar", edgecolor="black")
plt.xlabel("Genre")
plt.ylabel("Peak Average Popularity")
plt.title("Top-10 Genres: Peak Year & Popularity")
plt.xticks(rotation=45, ha="right")

for i, (genre, row) in enumerate(plot2.iterrows()):
    	ax.text(
        	i, 
        	row["peak_popularity"] + 0.5,
        	str(int(row["peak_year"])),
        	ha="center", va="bottom"
    	)
plt.tight_layout()
plt.show()

# track the average popularity for each year
peaks_for_overall = (
    yearly_top10
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select("genre","year","avg_popularity")
        .orderBy(F.desc("avg_popularity"))
    )

print("For each top-10 genre, the average popularity for each year:")
peaks_for_overall.show(truncate=False)

plot3 = yearly_top10.select("genre","year","avg_popularity").toPandas()
pivot = plot3.pivot(index="year", columns="genre", values="avg_popularity").sort_index()

# line graph
plt.figure()
for genre in pivot.columns:
    plt.plot(pivot.index, pivot[genre], marker="o", label=genre)

plt.xlabel("Year")
plt.ylabel("Average Popularity")
plt.title("Popularity Over Time for Top-10 Genres")
plt.legend(loc="best", bbox_to_anchor=(1.02, 1))
plt.tight_layout()
plt.show()
print(f"Task 2 runtime: {time.time() - start_time_task2:.2f} seconds")

# ---------------------------------------------------------------- TASK 3 ----------------------------------------------------------------
# task 3 - how has the number of explicit songs changed over time? are explicit songs more or less popular than non-explicit songs in the same genre? are explicit songs more or less popular than non-explicit songs in general? 
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

# ---------------------------------------------------------------- TASK 4 ----------------------------------------------------------------
# task 4 - what emotional tones are associated with songs that are considered “good for running,” “good for work/study,” and “good for relaxation/meditation”?
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

# ---------------------------------------------------------------- TASK 5 ----------------------------------------------------------------
# task 5 - what are the most significant features (ex: tempo, energy, danceability) that predict a song’s popularity?
spark = SparkSession.builder \
    .appName("Spotify Correlation Task 5") \
    .getOrCreate()

start_time_task5 = time.time()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.debug.maxToStringFields", 9)

df = spark.read.csv("updated_cleaned_dataset.csv", header=True, inferSchema=True)

from pyspark.ml.feature import StringIndexer
genre_indexer = StringIndexer(inputCol="Genre", outputCol="Genre_index", handleInvalid="skip")
explicit_indexer = StringIndexer(inputCol="Explicit", outputCol="Explicit_index", handleInvalid="skip")

df = genre_indexer.fit(df).transform(df)
df = explicit_indexer.fit(df).transform(df)

features = [
    "Genre_index", "Tempo", "Explicit_index", "Energy", "Danceability",
    "Positiveness", "Liveness", "Good for Party", "Good for Social Gatherings"
]

correlations = []
print("\nFeature Correlations (Pearson r with Popularity):")
for feat in features:
    cor_val = df.stat.corr(feat, "Popularity")
    correlations.append((feat, cor_val))
    print(f"{feat:25}: {cor_val:.4f}")

for feat in features:
    cor_val = df.stat.corr(feat, "Popularity")
    correlations.append((feat, cor_val))

coef_data = pd.DataFrame(correlations, columns=["Feature", "Coefficient"])
coef_data["AbsCoeff"] = coef_data["Coefficient"].abs()
coef_data.sort_values("AbsCoeff", ascending=True, inplace=True)

coef_data.to_csv("feature_importance_correlation.csv", index=False)

plt.figure(figsize=(10, 6))
plt.barh(coef_data["Feature"], coef_data["Coefficient"], color="steelblue")
plt.axvline(0, color="gray", linestyle="--")
plt.xlabel("Pearson Correlation with Popularity")
plt.title("Feature Correlation with Song Popularity")
plt.grid(True)
plt.tight_layout()
plt.savefig("feature_correlation.png")
plt.show()

print(f"\nTotal script runtime: {time.time() - start_time_task5:.2f} seconds")
