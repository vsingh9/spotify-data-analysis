# task 5 - what are the most significant features (e.g., tempo, energy, danceability) that predict a song’s popularity?
from pyspark.sql import SparkSession
from pyspark.sql.functions import corr
import matplotlib.pyplot as plt
import pandas as pd
import time

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
