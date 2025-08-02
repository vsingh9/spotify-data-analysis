# Spotify Data Analysis
The goal of this project was to analyze a 1 GB+ Spotify dataset using PySpark and Spark SQL to identify music trends across genres, emotions, popularity, and explicit content over time.

* Implemented big-data processing pipeline using Spark RDD transformations and DataFrames to handle complex multi-genre entries, perform temporal aggregation by release year, and conduct emotional tone analysis of various listening categories, visualizing results with the help of Pandas and Matplotlib

* Designed MySQL database schema with 10+ tables to store processed analytical results and wrote SQL queries for data insertion, retrieval, and filtering

* Performed feature importance analysis with 9 acoustic features using Pearson correlation to identify key predictiors of song popularity, finding social context features (party/social gathering suitability) as strongest popularity indicators

* Built interactive Flask web interface with MySQL backend and responsive frontend using JavaScript + HTML, enabling users to track popularity trends for top 10 genres, analyze explicit vs. non-explicit song performance, and query emotion distributions across different song categories through dynamic dropdowns
