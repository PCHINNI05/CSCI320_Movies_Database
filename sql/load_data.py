import psycopg2
import pandas as pd
import json
import configparser

config = configparser.ConfigParser()
config.read("../src/main/resources/db.properties")

url = config["DEFAULT"]["db.url"]
user = config["DEFAULT"]["db.user"]
password = config["DEFAULT"]["db.password"]

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname=url.split("/")[-1],
    user=user,
    password=password
)
cur = conn.cursor()

# ── Load MovieLens data ──────────────────────────────────────────

movies_ml = pd.read_csv("../data/ml-25m/movies.csv")
# movieId, title, genres

ratings = pd.read_csv("../data/ml-25m/ratings.csv")
# userId, movieId, rating, timestamp

links = pd.read_csv("../data/ml-25m/links.csv")
# movieId, imdbId, tmdbId

# ── Load TMDB data ───────────────────────────────────────────────

tmdb_movies = pd.read_csv("../data/tmdb_5000_movies.csv")
# id (=tmdbId), title, production_companies, runtime, vote_average...

tmdb_credits = pd.read_csv("../data/tmdb_5000_credits.csv")
# movie_id (=tmdbId), cast, crew

# ── Merge on tmdbId ──────────────────────────────────────────────

links["tmdbId"] = pd.to_numeric(links["tmdbId"], errors="coerce")
merged = movies_ml.merge(links, on="movieId")
merged = merged.merge(tmdb_movies, left_on="tmdbId", right_on="id", how="left")
merged = merged.merge(tmdb_credits, left_on="tmdbId", right_on="movie_id", how="left")

# ── Insert Genres ────────────────────────────────────────────────

all_genres = set()
for g in movies_ml["genres"]:
    for name in g.split("|"):
        all_genres.add(name.strip())

for genre_name in all_genres:
    cur.execute(
        "INSERT INTO genre (genre_name) VALUES (%s) ON CONFLICT DO NOTHING",
        (genre_name,)
    )

# ── Insert Studios ───────────────────────────────────────────────

for _, row in merged.iterrows():
    if pd.isna(row.get("production_companies")):
        continue
    try:
        companies = json.loads(row["production_companies"])
        for c in companies:
            cur.execute(
                "INSERT INTO studio (name) VALUES (%s) ON CONFLICT DO NOTHING",
                (c["name"],)
            )
    except:
        continue

# ── Insert Employees (cast + crew) ───────────────────────────────

for _, row in merged.iterrows():
    for col in ["cast", "crew"]:
        if pd.isna(row.get(col)):
            continue
        try:
            people = json.loads(row[col])
            for p in people:
                name_parts = p["name"].strip().split(" ", 1)
                first = name_parts[0]
                last = name_parts[1] if len(name_parts) > 1 else ""
                cur.execute(
                    """INSERT INTO employee (first_name, last_name)
                       VALUES (%s, %s)
                       ON CONFLICT DO NOTHING""",
                    (first, last)
                )
        except:
            continue

# ── Insert Movies ────────────────────────────────────────────────

for _, row in merged.iterrows():
    length = int(row["runtime"]) if not pd.isna(row.get("runtime")) and row["runtime"] > 0 else 90
    cur.execute(
        """INSERT INTO movie (movie_id, title, length, mpaa_rating)
           VALUES (%s, %s, %s, NULL)
           ON CONFLICT DO NOTHING""",
        (int(row["movieId"]), str(row["title_x"]), length)
    )

# ── Insert has_genre ─────────────────────────────────────────────

for _, row in merged.iterrows():
    for genre_name in str(row["genres"]).split("|"):
        genre_name = genre_name.strip()
        cur.execute("SELECT genre_id FROM genre WHERE genre_name = %s", (genre_name,))
        result = cur.fetchone()
        if result:
            cur.execute(
                "INSERT INTO has_genre (movie_id, genre_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (int(row["movieId"]), result[0])
            )

# ── Insert acts_in + directs ─────────────────────────────────────

for _, row in merged.iterrows():
    movie_id = int(row["movieId"])

    # Cast → acts_in
    if not pd.isna(row.get("cast")):
        try:
            for p in json.loads(row["cast"]):
                name_parts = p["name"].strip().split(" ", 1)
                first, last = name_parts[0], (name_parts[1] if len(name_parts) > 1 else "")
                cur.execute("SELECT employee_id FROM employee WHERE first_name=%s AND last_name=%s", (first, last))
                emp = cur.fetchone()
                if emp:
                    cur.execute("INSERT INTO acts_in VALUES (%s,%s) ON CONFLICT DO NOTHING", (movie_id, emp[0]))
        except:
            continue

    # Crew → directs (directors only)
    if not pd.isna(row.get("crew")):
        try:
            for p in json.loads(row["crew"]):
                if p.get("job") == "Director":
                    name_parts = p["name"].strip().split(" ", 1)
                    first, last = name_parts[0], (name_parts[1] if len(name_parts) > 1 else "")
                    cur.execute("SELECT employee_id FROM employee WHERE first_name=%s AND last_name=%s", (first, last))
                    emp = cur.fetchone()
                    if emp:
                        cur.execute("INSERT INTO directs VALUES (%s,%s) ON CONFLICT DO NOTHING", (movie_id, emp[0]))
        except:
            continue

# ── Insert Ratings ───────────────────────────────────────────────

# First insert users from ratings (MovieLens has no user details, just IDs)
user_ids = ratings["userId"].unique()
for uid in user_ids:
    cur.execute(
        """INSERT INTO users (user_id, first_name, last_name, username, email, password)
           VALUES (%s, 'User', %s, %s, %s, 'placeholder')
           ON CONFLICT DO NOTHING""",
        (int(uid), str(uid), f"user{uid}", f"user{uid}@movielens.com")
    )

# Then insert ratings in chunks (25M rows — do in batches)
chunk_size = 10000
for i in range(0, len(ratings), chunk_size):
    chunk = ratings.iloc[i:i+chunk_size]
    for _, r in chunk.iterrows():
        star = min(5, max(1, round(float(r["rating"]))))
        cur.execute(
            "INSERT INTO rates VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (int(r["userId"]), int(r["movieId"]), star)
        )
    conn.commit()  # commit each chunk
    print(f"Ratings progress: {i}/{len(ratings)}")

conn.commit()
cur.close()
conn.close()
print("Done!")