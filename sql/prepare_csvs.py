import pandas as pd
import json

# ────────────── Load ──────────────
movies_ml = pd.read_csv("../data/ml-25m/movies.csv")
ratings   = pd.read_csv("../data/ml-25m/ratings.csv")
links     = pd.read_csv("../data/ml-25m/links.csv")
tmdb_movies  = pd.read_csv("../data/tmdb_5000_movies.csv")
tmdb_credits = pd.read_csv("../data/tmdb_5000_credits.csv")

# ────────────── Merge ──────────────
links["tmdbId"] = pd.to_numeric(links["tmdbId"], errors="coerce")
merged = movies_ml.merge(links, on="movieId")
merged = merged.merge(tmdb_movies, left_on="tmdbId", right_on="id", how="left")
merged = merged.merge(tmdb_credits, left_on="tmdbId", right_on="movie_id", how="left")

# ────────────── genre.csv ──────────────
all_genres = set()
for g in movies_ml["genres"]:
    for name in g.split("|"):
        all_genres.add(name.strip())
pd.DataFrame({"genre_name": sorted(all_genres)}).to_csv("../data/out/genre.csv", index=False)

# ────────────── studio.csv ──────────────
studios = set()
for _, row in merged.iterrows():
    try:
        for c in json.loads(row["production_companies"]):
            studios.add(c["name"])
    except: continue
pd.DataFrame({"name": sorted(studios)}).to_csv("../data/out/studio.csv", index=False)

# ────────────── employee.csv ──────────────
employees = set()
for _, row in merged.iterrows():
    for col in ["cast", "crew"]:
        try:
            for p in json.loads(row[col]):
                parts = p["name"].strip().split(" ", 1)
                employees.add((parts[0], parts[1] if len(parts) > 1 else ""))
        except: continue
pd.DataFrame(list(employees), columns=["first_name","last_name"]).to_csv("../data/out/employee.csv", index=False)

# ────────────── movie.csv ──────────────
movies_out = []
for _, row in merged.iterrows():
    length = int(row["runtime"]) if not pd.isna(row.get("runtime")) and row["runtime"] > 0 else 90
    movies_out.append({"movie_id": int(row["movieId"]), "title": str(row["title_x"]), "length": length, "mpaa_rating": None})
pd.DataFrame(movies_out).drop_duplicates("movie_id").to_csv("../data/out/movie.csv", index=False)

# ──────────────── has_genre.csv ──────────────
genre_df = pd.read_csv("../data/out/genre.csv").reset_index()
genre_df.columns = ["genre_id", "genre_name"]
genre_df["genre_id"] += 1
has_genre = []
for _, row in merged.iterrows():
    for g in str(row["genres_x"]).split("|"):
        match = genre_df[genre_df["genre_name"] == g.strip()]
        if not match.empty:
            has_genre.append({"movie_id": int(row["movieId"]), "genre_id": int(match.iloc[0]["genre_id"])})
pd.DataFrame(has_genre).drop_duplicates().to_csv("../data/out/has_genre.csv", index=False)

# ────────────── users.csv ──────────────
user_ids = ratings["userId"].unique()
users_out = [{"user_id": int(uid), "first_name": "User", "last_name": str(uid),
              "username": f"user{uid}", "email": f"user{uid}@movielens.com",
              "password": "placeholder"} for uid in user_ids]
pd.DataFrame(users_out).to_csv("../data/out/users.csv", index=False)

# ────────────── rates.csv ──────────────
rates_out = ratings[["userId","movieId","rating"]].copy()
rates_out["star_rating"] = rates_out["rating"].apply(lambda x: min(5, max(1, round(float(x)))))
rates_out = rates_out[["userId","movieId","star_rating"]]
rates_out.columns = ["user_id","movie_id","star_rating"]
rates_out.to_csv("../data/out/rates.csv", index=False)

print("All CSVs written to data/out/")