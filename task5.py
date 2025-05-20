# task 5 - what are the most significant features (e.g., tempo, energy, danceability) that predict a song’s popularity?
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder \
        .appName("Spotify Analysis Task 5") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.debug.maxToStringFields", 9) 

df = spark.read.csv("updated_cleaned_dataset.csv", header=True, inferSchema=True)

genre_indexer = StringIndexer(inputCol="Genre", outputCol="Genre_index", handleInvalid="skip")
explicit_indexer = StringIndexer(inputCol="Explicit", outputCol="Explicit_index", handleInvalid="skip")

features = ["Genre_index", "Tempo", "Explicit_index", "Energy", "Danceability", "Positiveness", "Liveness", "Good for Party", "Good for Social Gatherings"]

assembler = VectorAssembler(inputCols=features, outputCol="features_unscaled")
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True)

lr = LinearRegression(featuresCol="features", labelCol="Popularity")

pipeline = Pipeline(stages=[genre_indexer, explicit_indexer, assembler, scaler, lr])
model = pipeline.fit(df)
lr_model = model.stages[-1]

print("\nFeature Importances (Linear Regression Coefficients):")
for coef, feat in sorted(zip(lr_model.coefficients, features), key=lambda x: abs(x[0]), reverse=True):
    print(f"{feat:20}: {coef:.4f}")
coef_data = pd.DataFrame({
    "Feature": features,
    "Coefficient": [float(c) for c in lr_model.coefficients]
})

coef_data["AbsCoeff"] = coef_data["Coefficient"].abs()
coef_data.sort_values("AbsCoeff", ascending=False, inplace=True)

df.write.csv("feature_importance_output", header=True, mode="overwrite")
                                                                                                                                                                                                         
plt.barh(coef_data["Feature"], coef_data["Coefficient"])
plt.xlabel("Coefficient Value")
plt.title("Feature Importance in Predicting Song Popularity")
plt.grid(True)
plt.savefig("feature_importance.png")
plt.show()
