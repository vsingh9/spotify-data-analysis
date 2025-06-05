from flask import Flask, render_template, jsonify, request
import mysql.connector

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cs179g_db",
        # port=5000
    )

@app.route('/')
def index():
    return render_template("index.html")

# ---------------------------------------- TASK 1 ----------------------------------------
@app.route('/get_genre_emotion_count', methods=['GET'])
def get_genre_emotion_count():
    db = get_db_connection()
    cursor=db.cursor()
    query = """
        SELECT genre, emotion, count 
        FROM genre_emotion_count;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return jsonify({"genre_emotion_count":rows}), 200

@app.route('/get_genres', methods=['GET'])
def get_genres():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT DISTINCT genre
        FROM genre_emotion_count
        ORDER BY genre;
    """
    cursor.execute(query)
    genres = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()

    return jsonify(genres)

@app.route('/get_emotions', methods=['GET'])
def get_emotions():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT DISTINCT emotion
        FROM genre_emotion_count
        ORDER BY emotion;
    """
    cursor.execute(query)
    emotions = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()

    return jsonify(emotions)

@app.route('/filter_count', methods=['GET'])
def filter_count():
    min_count = request.args.get("min_count", type=int)
    if min_count is None:
        return jsonify({"error": "Missing min_count parameter"}), 400

    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT *
        FROM genre_emotion_count
        WHERE `count` > %s;
    """
    cursor.execute(query, (min_count,))
    genre_emotion_count = [row for row in cursor.fetchall()]

    cursor.close()
    db.close()

    return jsonify(genre_emotion_count)

# ---------------------------------------- TASK 2 ----------------------------------------
@app.route('/get_all_trends', methods=['GET'])
def get_all_trends():
    db = get_db_connection()
    cursor = db.cursor()

    query = "SELECT genre, year, avg_popularity FROM trends_for_top10_genre ORDER BY genre, year;"
    cursor.execute(query)
    data = cursor.fetchall()

    cursor.close()
    db.close()

    result = [
        {"genre": row[0], "year": row[1], "popularity": row[2]}
        for row in data
    ]
    return jsonify(result)

@app.route('/get_distinct_genres', methods=['GET'])
def get_distinct_genres():
    db = get_db_connection()
    cursor = db.cursor()
    
    query = """
        SELECT DISTINCT genre
        FROM trends_for_top10_genre
        ORDER BY genre;
    """
    cursor.execute(query)
    genres = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()
    return jsonify(genres)


@app.route('/get_peak_for_genre/<genre>', methods=['GET'])
def get_peak_for_genre(genre):
    db = get_db_connection()
    cursor = db.cursor()
    query = """
        SELECT year, avg_popularity
        FROM trends_for_top10_genre
        WHERE genre = %s
        ORDER BY avg_popularity DESC
        LIMIT 1
    """
    cursor.execute(query, (genre,))
    row = cursor.fetchone()
    cursor.close()
    db.close()

    if row:
        return jsonify({"year": row[0], "popularity": row[1]})
    else:
        return jsonify({"error": "Genre not found"}), 404


# ---------------------------------------- TASK 3 ----------------------------------------
@app.route('/get_explicit_genres', methods=['GET'])
def get_explicit_genres():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT DISTINCT genre
        FROM explicit_genre_popularity
        ORDER BY genre;
    """
    cursor.execute(query)
    genres = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()
    return jsonify(genres)

@app.route('/get_explicit_popularity_by_genre/<genre>', methods=['GET'])
def get_explicit_popularity_by_genre(genre):
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT explicit, avg_popularity
        FROM explicit_genre_popularity
        WHERE genre = %s
        ORDER BY explicit;
    """
    cursor.execute(query, (genre,))
    data = cursor.fetchall()

    cursor.close()
    db.close()

    result = [
        {"explicit": bool(row[0]), "popularity": row[1]}
        for row in data
    ]
    return jsonify(result)

# ---------------------------------------- TASK 4 ----------------------------------------
@app.route('/get_categories', methods=['GET'])
def get_categories():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT DISTINCT category
        FROM category_names;
    """
    cursor.execute(query)
    categories = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()

    return jsonify(categories)

@app.route('/get_emotions_4', methods=['GET'])
def get_emotions_4():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT DISTINCT emotion
        FROM run_emotion_counts
        ORDER BY emotion;
    """
    cursor.execute(query)
    emotions = [row[0] for row in cursor.fetchall()]

    cursor.close()
    db.close()

    return jsonify(emotions)

@app.route('/get_run_emotion_count', methods=['GET'])
def get_run_emotion_count():
    db = get_db_connection()
    cursor=db.cursor()
    query = """
        SELECT emotion, count 
        FROM run_emotion_counts;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return jsonify({"run_emotion_counts":rows}), 200

@app.route('/get_study_emotion_count', methods=['GET'])
def get_study_emotion_count():
    db = get_db_connection()
    cursor=db.cursor()
    query = """
        SELECT emotion, count 
        FROM study_emotion_counts;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return jsonify({"study_emotion_counts":rows}), 200

@app.route('/get_relax_emotion_count', methods=['GET'])
def get_relax_emotion_count():
    db = get_db_connection()
    cursor=db.cursor()
    query = """
        SELECT emotion, count 
        FROM relax_emotion_counts;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    return jsonify({"relax_emotion_counts":rows}), 200

# ---------------------------------------- TASK 5 ----------------------------------------
@app.route('/get_feature_importance', methods=['GET'])
def get_feature_importance():
    db = get_db_connection()
    cursor = db.cursor()

    query = """
        SELECT feature, coefficient
        FROM feature_importance
        ORDER BY ABS(coefficient) DESC;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    db.close()

    result = [
        {"feature": row[0], "coefficient": row[1]}
        for row in rows
    ]

    return jsonify(result)

if __name__ == "__main__":
    print("connecting to DB...")
    app.run(host="0.0.0.0", port=5002, debug=True)
