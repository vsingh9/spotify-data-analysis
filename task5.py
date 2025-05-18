# task 5 - what are the most significant features (e.g., tempo, energy, danceability) that predict a song’s popularity?
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

df = spark.read.csv("updated_cleaned_dataset.csv", header=True, inferSchema=Tru>
df.selectfrom pyspark.sql import SparkSession

features = [
    "Genre", "Tempo", "Explicit", "Energy", "Danceability",
    "Positiveness", "Liveness", "Good for Party", "Good for Social Gatherings"
]

assembler = VectorAssembler(inputCols=features, outputCol="features_unscaled")
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", wit>

lr = LinearRegression(featuresCol="features", labelCol="Popularity")

pipeline = Pipeline(stages=[assembler, scaler, lr])
model = pipeline.fit(df_clean)
lr_model = model.stages[-1]

print("\nFeature Importances (Linear Regression Coefficients):")
for coef, feat in sorted(zip(lr_model.coefficients, features), key=lambda x: ab>
    print(f"{feat:20}: {coef:.4f}")
