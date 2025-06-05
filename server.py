# Load data into MySQL database
import mysql.connector
import csv 

db = mysql.connector.connect(
    # host="localhost",
    unix_socket="/home/cs179g/mysql.sock",
    user="root",
    password="",
    database="cs179g_db",
    # port=5000
)

cursor = db.cursor()

# ---------------------------------------- TASK 1 ----------------------------------------
csv_data_1 = csv.reader(open('genre_emotion_counts.csv'))

# Create Table Statement
create_table_query_1 = """
    CREATE TABLE genre_emotion_count (
        genre VARCHAR(50),
        emotion VARCHAR(50),
        count INT
    );
"""

# Insert Query Statement
insert_query_1 = """
    INSERT INTO genre_emotion_count (genre, emotion, count)
    VALUES (%s, %s, %s);
"""

# for row in csv_data_1:
#     cursor.execute(insert_query_1, row)
#     print(row)

# Select Query Statement
select_query_1 = """
    SELECT * FROM genre_emotion_count;
"""

cursor.execute(select_query_1)
rows = cursor.fetchall()
for row in rows:
    print(row)

# ---------------------------------------- TASK 2 ----------------------------------------
csv_data_2 = csv.reader(open('peaks_for_overall.csv'))

cursor.execute("DROP TABLE IF EXISTS trends_for_top10_genre;")
# Create new table 
create_table_query_2 = """
    CREATE TABLE  IF NOT EXISTS trends_for_top10_genre (
        genre VARCHAR(100),
        year INT,
        avg_popularity FLOAT
    ); 
"""

cursor.execute(create_table_query_2)

# Insert query
insert_query_2 = """
    INSERT INTO trends_for_top10_genre (genre, year, avg_popularity)
    VALUES (%s, %s, %s)
"""
next(csv_data_2)

for row in csv_data_2:
    genre = row[0]
    year = int(row[1])
    avg_popularity = float(row[2])
    cursor.execute(insert_query_2, (genre, year, avg_popularity))
    
    
# Select Query Statement
select_query_2 = """
    SELECT * FROM trends_for_top10_genre;
"""

print("\n--- Dataset for peaks_for_top10 (task 2) ---")
cursor.execute(select_query_2)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
print("\n---------------------------------")

# ---------------------------------------- TASK 3 ----------------------------------------
csv_data_3 = csv.reader(open('genre_popularity_explicit.csv'))

# Drop existing table if it exists
cursor.execute("DROP TABLE IF EXISTS explicit_genre_popularity;")

# Create new table for explicit song genre popularity
create_table_query = """
    CREATE TABLE IF NOT EXISTS explicit_genre_popularity (
        genre VARCHAR(100),
        explicit BOOLEAN,
        avg_popularity FLOAT
    );
"""
cursor.execute(create_table_query)

# Prepare insert query
insert_query = """
    INSERT INTO explicit_genre_popularity (genre, explicit, avg_popularity)
    VALUES (%s, %s, %s)
"""

# Skip header
next(csv_data_3)

# Insert each row
for row in csv_data_3:
    genre = row[0]
    explicit = row[1].strip().lower() in ('true', '1')
    avg_popularity = float(row[2])
    cursor.execute(insert_query, (genre, explicit, avg_popularity))

# Select and print results
select_query = "SELECT * FROM explicit_genre_popularity;"
print("\n--- Dataset for explicit_genre_popularity ---")
cursor.execute(select_query)
rows = cursor.fetchall()
for row in rows:
    print(row)

# ---------------------------------------- TASK 4 ----------------------------------------
csv_data_4_1 = csv.reader(open('task4_db/run_emotion_counts.csv'))
csv_data_4_2 = csv.reader(open('task4_db/study_emotion_counts.csv'))
csv_data_4_3 = csv.reader(open('task4_db/relax_emotion_counts.csv'))
csv_data_4_4 = csv.reader(open('task4_db/relax_tempo_avg.csv'))
csv_data_4_5 = csv.reader(open('task4_db/run_tempo_avg.csv'))
csv_data_4_6 = csv.reader(open('task4_db/study_tempo_avg.csv'))
csv_data_4_7 = csv.reader(open('task4_db/category_names.csv'))

# Create Table Statements
create_table_query_4_1 = """
    CREATE TABLE IF NOT EXISTS run_emotion_counts (
        emotion VARCHAR(10),
        count INT
    );
"""

create_table_query_4_2 = """
    CREATE TABLE IF NOT EXISTS study_emotion_counts (
        emotion VARCHAR(10),
        count INT
    );
"""

create_table_query_4_3 = """
    CREATE TABLE IF NOT EXISTS relax_emotion_counts (
        emotion VARCHAR(10),
        count INT
    );
"""

create_table_query_4_4 = """
    CREATE TABLE IF NOT EXISTS relax_tempo_avg (
        `Good for Relaxation/Meditation` VARCHAR(35),
        avg_Tempo FLOAT
    );
"""

create_table_query_4_5 = """
    CREATE TABLE IF NOT EXISTS run_tempo_avg (
        `Good for Running` VARCHAR(35),
        avg_Tempo FLOAT
    );
"""

create_table_query_4_6 = """
    CREATE TABLE IF NOT EXISTS study_tempo_avg (
        `Good for Work/Study` VARCHAR(35),
        avg_Tempo FLOAT
    );
"""

create_table_query_4_7 = """
    CREATE TABLE IF NOT EXISTS category_names (
        category VARCHAR(35)
    );
"""

cursor.execute(create_table_query_4_1)
cursor.execute(create_table_query_4_2)
cursor.execute(create_table_query_4_3)
cursor.execute(create_table_query_4_4)
cursor.execute(create_table_query_4_5)
cursor.execute(create_table_query_4_6)
cursor.execute(create_table_query_4_7)

# Insert Query Statements
insert_query_4_1 = """
    INSERT INTO run_emotion_counts (emotion, count)
    VALUES (%s, %s);
"""
for row in csv_data_4_1:
    cursor.execute(insert_query_4_1, row)
    print(row)

insert_query_4_2 = """
    INSERT INTO study_emotion_counts (emotion, count)
    VALUES (%s, %s);
"""
for row in csv_data_4_2:
    cursor.execute(insert_query_4_2, row)
    print(row)

insert_query_4_3 = """
    INSERT INTO relax_emotion_counts (emotion, count)
    VALUES (%s, %s);
"""
for row in csv_data_4_3:
    cursor.execute(insert_query_4_3, row)
    print(row)

insert_query_4_4 = """
    INSERT INTO relax_tempo_avg (`Good for Relaxation/Meditation`, avg_Tempo)
    VALUES (%s, %s);
"""
for row in csv_data_4_4:
    cursor.execute(insert_query_4_4, row)
    print(row)

insert_query_4_5 = """
    INSERT INTO run_tempo_avg (`Good for Running`, avg_Tempo)
    VALUES (%s, %s);
"""
for row in csv_data_4_5:
    cursor.execute(insert_query_4_5, row)
    print(row)

insert_query_4_6 = """
    INSERT INTO study_tempo_avg (`Good for Work/Study`, avg_Tempo)
    VALUES (%s, %s);
"""
for row in csv_data_4_6:
    cursor.execute(insert_query_4_6, row)
    print(row)
    
insert_query_4_7 = """
    INSERT INTO category_names (category)
    VALUES (%s);
"""
for row in csv_data_4_7:
    cursor.execute(insert_query_4_7, row)
    print(row)

# Select Query Statements
select_query_4_1 = """
    SELECT * FROM run_emotion_counts;
"""

cursor.execute(select_query_4_1)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
select_query_4_2 = """
    SELECT * FROM study_emotion_counts;
"""

cursor.execute(select_query_4_2)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
select_query_4_3 = """
    SELECT * FROM relax_emotion_counts;
"""

cursor.execute(select_query_4_3)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
select_query_4_4 = """
    SELECT * FROM relax_tempo_avg;
"""

cursor.execute(select_query_4_4)
rows = cursor.fetchall()
for row in rows:
    print(row)

select_query_4_5 = """
    SELECT * FROM run_tempo_avg;
"""

cursor.execute(select_query_4_5)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
select_query_4_6 = """
    SELECT * FROM study_tempo_avg;
"""

cursor.execute(select_query_4_6)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
select_query_4_7 = """
    SELECT * FROM category_names;
"""

cursor.execute(select_query_4_7)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
# ---------------------------------------- TASK 5 ----------------------------------------
csv_data_5 = csv.reader(open('feature_importance_correlation.csv')) 

cursor.execute("DROP TABLE IF EXISTS feature_importance;")

create_table_query_5 = """
    CREATE TABLE IF NOT EXISTS feature_importance (
        feature VARCHAR(100),
        coefficient FLOAT
    );
"""
cursor.execute(create_table_query_5)

insert_query_5 = """
    INSERT INTO feature_importance (feature, coefficient)
    VALUES (%s, %s)
"""
next(csv_data_5)

for row in csv_data_5:
    feature = row[0]
    coefficient = float(row[1])
    cursor.execute(insert_query_5, (feature, coefficient))

select_query_5 = """
    SELECT feature, coefficient
    FROM feature_importance
    ORDER BY ABS(coefficient) DESC;
"""

print("\n--- Task 5: Feature Importance for Song Popularity ---")
cursor.execute(select_query_5)
rows = cursor.fetchall()
for row in rows:
    print(f"{row[0]:30}: {row[1]:.4f}")

print("\n---------------------------------")

db.commit()
cursor.close()
db.close()
