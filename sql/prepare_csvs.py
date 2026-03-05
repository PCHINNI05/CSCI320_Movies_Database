import pandas as pd
import json
from faker import Faker
import random
import pickle

fake = Faker()
Faker.seed(42)
random.seed(42)

# ────────────── Config ──────────────
NUM_USERS   = 10_000
RATES_CAP   = 100_000

# ────────────── Load ──────────────
movies_ml    = pd.read_csv("../data/ml-25m/movies.csv")
ratings      = pd.read_csv("../data/ml-25m/ratings.csv")
links        = pd.read_csv("../data/ml-25m/links.csv")
tmdb_movies  = pd.read_csv("../data/tmdb_5000_movies.csv")
tmdb_credits = pd.read_csv("../data/tmdb_5000_credits.csv")

# ────────────── Merge ──────────────
links["tmdbId"] = pd.to_numeric(links["tmdbId"], errors="coerce")
merged = movies_ml.merge(links, on="movieId")
merged = merged.merge(tmdb_movies, left_on="tmdbId", right_on="id", how="left")
merged = merged.merge(tmdb_credits, left_on="tmdbId", right_on="movie_id", how="left")

# ────────────── genre.csv (no id col - SERIAL assigns) ──────────────
all_genres = set()
for g in movies_ml["genres"]:
    for name in g.split("|"):
        all_genres.add(name.strip())
genre_list = sorted(all_genres)
pd.DataFrame({"genre_name": genre_list}).to_csv("../data/out/genre.csv", index=False)
# genre name --> sequential id (position + 1 matches SERIAL insert order)
genre_id_map = {name: i + 1 for i, name in enumerate(genre_list)}
print(f"genre.csv done - {len(genre_list)} rows")

# ────────────── studio.csv (no id col - SERIAL assigns) ──────────────
studios = set()
for _, row in merged.iterrows():
    try:
        for c in json.loads(row["production_companies"]):
            studios.add(c["name"])
    except:
        continue
studio_list = sorted(studios)
pd.DataFrame({"name": studio_list}).to_csv("../data/out/studio.csv", index=False)
studio_id_map = {name: i + 1 for i, name in enumerate(studio_list)}
print(f"studio.csv done - {len(studio_list)} rows")

# ────────────── employee.csv (no id col - SERIAL assigns) ──────────────
employees = set()
for _, row in merged.iterrows():
    for col in ["cast", "crew"]:
        try:
            for p in json.loads(row[col]):
                parts = p["name"].strip().split(" ", 1)
                first = parts[0] if parts[0] else "Unknown"
                last  = parts[1] if len(parts) > 1 and parts[1].strip() else " "
                employees.add((first, last))
        except:
            continue
employee_list = sorted(employees)
emp_df = pd.DataFrame(employee_list, columns=["first_name", "last_name"])
emp_df["first_name"] = emp_df["first_name"].fillna("Unknown")
emp_df["last_name"]  = emp_df["last_name"].fillna(" ").replace("", " ")
emp_df.to_csv("../data/out/employee.csv", index=False)
# (first, last) --> sequential id
employee_id_map = {(row["first_name"], row["last_name"]): i + 1
                   for i, (_, row) in enumerate(emp_df.iterrows())}
print(f"employee.csv done - {len(emp_df):,} rows")

# ────────────── movie.csv (no id col - SERIAL assigns) ──────────────
movies_out = []
seen_orig_ids = set()
for _, row in merged.iterrows():
    orig_id = int(row["movieId"])
    if orig_id in seen_orig_ids:
        continue
    seen_orig_ids.add(orig_id)
    title  = str(row["title_x"]) if not pd.isna(row.get("title_x")) else str(row.get("title", "Unknown"))
    length = int(row["runtime"]) if not pd.isna(row.get("runtime")) and row["runtime"] > 0 else 90
    movies_out.append({"_orig_id": orig_id, "title": title, "length": length, "mpaa_rating": None})

movie_build_df = pd.DataFrame(movies_out).reset_index(drop=True)
# orig movieId --> sequential id (row index + 1 matches SERIAL insert order)
movie_id_map = {int(row["_orig_id"]): i + 1
                for i, (_, row) in enumerate(movie_build_df.iterrows())}
movie_build_df.drop(columns=["_orig_id"]).to_csv("../data/out/movie.csv", index=False)
print(f"movie.csv done - {len(movie_build_df):,} rows")

# ────────────── has_genre.csv ──────────────
has_genre = []
for _, row in merged.iterrows():
    seq_movie_id = movie_id_map.get(int(row["movieId"]))
    if seq_movie_id is None:
        continue
    for g in str(row["genres_x"]).split("|"):
        seq_genre_id = genre_id_map.get(g.strip())
        if seq_genre_id:
            has_genre.append({"movie_id": seq_movie_id, "genre_id": seq_genre_id})
has_genre_df = pd.DataFrame(has_genre).drop_duplicates()
has_genre_df.to_csv("../data/out/has_genre.csv", index=False)
print(f"has_genre.csv done - {len(has_genre_df):,} rows")

# ────────────── users.csv (no id col - SERIAL assigns) ──────────────
top_user_ids = (
    ratings.groupby("userId").size()
    .nlargest(NUM_USERS)
    .index.tolist()
)
top_user_ids.sort()

domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
users_out = []
for orig_uid in top_user_ids:
    first    = fake.first_name()
    last     = fake.last_name()
    username = f"{first.lower()}{last.lower()}{orig_uid}"[:50]
    email    = f"{first.lower()}{orig_uid}@{random.choice(domains)}"[:150]
    users_out.append({
        "first_name": first,
        "last_name":  last,
        "username":   username,
        "email":      email,
        "password":   fake.sha256()
    })
pd.DataFrame(users_out).to_csv("../data/out/users.csv", index=False)
# orig userId --> sequential id
user_id_map = {orig_uid: i + 1 for i, orig_uid in enumerate(top_user_ids)}
print(f"users.csv done - {len(users_out):,} rows")

# ────────────── rates.csv ──────────────
valid_orig_movie_ids = set(movie_id_map.keys())
rates_filtered = ratings[ratings["userId"].isin(top_user_ids)].copy()
rates_filtered = rates_filtered[rates_filtered["movieId"].isin(valid_orig_movie_ids)]
if len(rates_filtered) > RATES_CAP:
    rates_filtered = rates_filtered.sample(n=RATES_CAP, random_state=42)
rates_filtered["star_rating"] = rates_filtered["rating"].apply(
    lambda x: min(5, max(1, round(float(x))))
)
rates_filtered["user_id"]  = rates_filtered["userId"].map(user_id_map)
rates_filtered["movie_id"] = rates_filtered["movieId"].map(movie_id_map)
rates_filtered = rates_filtered[["user_id", "movie_id", "star_rating"]].dropna()
rates_filtered = rates_filtered.drop_duplicates(subset=["user_id", "movie_id"])
rates_filtered["user_id"]  = rates_filtered["user_id"].astype(int)
rates_filtered["movie_id"] = rates_filtered["movie_id"].astype(int)
rates_filtered.to_csv("../data/out/rates.csv", index=False)
print(f"rates.csv done - {len(rates_filtered):,} rows")

# ────────────── Save ID maps for pass2 + pass3 ──────────────
with open("../data/out/id_maps.pkl", "wb") as f:
    pickle.dump({
        "movie_id_map":    movie_id_map,
        "user_id_map":     user_id_map,
        "genre_id_map":    genre_id_map,
        "studio_id_map":   studio_id_map,
        "employee_id_map": employee_id_map,
    }, f)
print("id_maps.pkl saved")
print("\nAll pass-1 CSVs written to data/out/")