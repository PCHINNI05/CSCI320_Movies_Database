import pandas as pd
import json
import random

# ────────────── Load Generated CSVs (for ID lookup) ──────────────
employee_df = pd.read_csv("../data/out/employee.csv").reset_index()
employee_df.columns = ["employee_id", "first_name", "last_name"]
employee_df["employee_id"] += 1
employee_lookup = {
    (row["first_name"], row["last_name"]): int(row["employee_id"])
    for _, row in employee_df.iterrows()
}

studio_df = pd.read_csv("../data/out/studio.csv").reset_index()
studio_df.columns = ["studio_id", "name"]
studio_df["studio_id"] += 1

movie_df = pd.read_csv("../data/out/movie.csv")

# ────────────── Load Raw Source Data ──────────────
movies_ml    = pd.read_csv("../data/ml-25m/movies.csv")
links        = pd.read_csv("../data/ml-25m/links.csv")
tmdb_movies  = pd.read_csv("../data/tmdb_5000_movies.csv")
tmdb_credits = pd.read_csv("../data/tmdb_5000_credits.csv")

# ────────────── Merge ──────────────
links["tmdbId"] = pd.to_numeric(links["tmdbId"], errors="coerce")
merged = movies_ml.merge(links, on="movieId")
merged = merged.merge(tmdb_movies, left_on="tmdbId", right_on="id", how="left")
merged = merged.merge(tmdb_credits, left_on="tmdbId", right_on="movie_id", how="left")

# ────────────── Helper ──────────────
def get_employee_id(first, last):
    return employee_lookup.get((first, last))

# ────────────── acts_in.csv ──────────────
acts_in = []
for _, row in merged.iterrows():
    movie_id = int(row["movieId"])
    if pd.isna(row.get("cast")):
        continue
    try:
        for p in json.loads(row["cast"]):
            parts = p["name"].strip().split(" ", 1)
            first, last = parts[0], (parts[1] if len(parts) > 1 else "")
            eid = get_employee_id(first, last)
            if eid:
                acts_in.append({"movie_id": movie_id, "employee_id": eid})
    except:
        continue
pd.DataFrame(acts_in).drop_duplicates().to_csv("../data/out/acts_in.csv", index=False)
print("acts_in.csv done")

# ────────────── directs.csv ──────────────
directs = []
for _, row in merged.iterrows():
    movie_id = int(row["movieId"])
    if pd.isna(row.get("crew")):
        continue
    try:
        for p in json.loads(row["crew"]):
            if p.get("job") == "Director":
                parts = p["name"].strip().split(" ", 1)
                first, last = parts[0], (parts[1] if len(parts) > 1 else "")
                eid = get_employee_id(first, last)
                if eid:
                    directs.append({"movie_id": movie_id, "employee_id": eid})
    except:
        continue
pd.DataFrame(directs).drop_duplicates().to_csv("../data/out/directs.csv", index=False)
print("directs.csv done")

# ────────────── produces.csv ──────────────
produces = []
for _, row in merged.iterrows():
    movie_id = int(row["movieId"])
    if pd.isna(row.get("production_companies")):
        continue
    try:
        for c in json.loads(row["production_companies"]):
            match = studio_df[studio_df["name"] == c["name"]]
            if not match.empty:
                produces.append({
                    "movie_id": movie_id,
                    "studio_id": int(match.iloc[0]["studio_id"])
                })
    except:
        continue
pd.DataFrame(produces).drop_duplicates().to_csv("../data/out/produces.csv", index=False)
print("produces.csv done")

# ────────────── has_platform.csv ──────────────
# No real platform data exists in either dataset, so we generate it.
# Each movie gets 1-3 random platforms with a realistic release date.
platforms = ["Netflix", "Hulu", "Amazon Prime", "Disney+", "HBO Max"]
platform_ids = {name: i+1 for i, name in enumerate(platforms)}

# Write platform.csv too so DataGrip import matches IDs
pd.DataFrame([{"platform_id": v, "platform_name": k} for k, v in platform_ids.items()]) \
    .to_csv("../data/out/platform.csv", index=False)

has_platform = []
for movie_id in movie_df["movie_id"]:
    assigned = random.sample(platforms, k=random.randint(1, 3))
    for p in assigned:
        has_platform.append({
            "movie_id": int(movie_id),
            "platform_id": platform_ids[p],
            "release_date": f"{random.randint(2010, 2023)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        })
        
pd.DataFrame(has_platform).drop_duplicates(subset=["movie_id","platform_id"]) \
    .to_csv("../data/out/has_platform.csv", index=False)

print("has_platform.csv + platform.csv done")

print("\nAll pass-2 CSVs written to data/out/")