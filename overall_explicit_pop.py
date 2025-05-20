import pandas as pd

df = pd.read_csv("updated_cleaned_dataset.csv")
# Calculate average popularity per explicit category
avg_popularity = df.groupby('Explicit')['Popularity'].mean()

print(avg_popularity)

