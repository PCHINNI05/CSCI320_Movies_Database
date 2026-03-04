import pandas as pd
import json
import random
import pickle

random.seed(42)

# ────────────── Config ──────────────
ACTS_IN_CAP  = 150_000
DIRECTS_CAP  =  60_000
PRODUCES_CAP =  60_000

# ────────────── Load ID maps from pass1 ──────────────
with open("../data/out/id_maps.pkl", "rb") as f:
    maps = pickle.load(f)

movie_id_map    = maps["movie_id_map"]     # orig movieId    --> sequential id
studio_id_map   = maps["studio_id_map"]    # studio name     --> sequential id
employee_id_map = maps["employee_id_map"]  # (first, last)   --> sequential id

valid_seq_movie_ids = set(movie_id_map.values())

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
    # Normalise to match what pass1 stored
    first = first if first else "Unknown"
    last  = last if last and last.strip() else " "
    return employee_id_map.get((first, last))

# ────────────── acts_in.csv ──────────────
acts_in = []
for _, row in merged.iterrows():
    seq_movie_id = movie_id_map.get(int(row["movieId"]))
    if seq_movie_id is None or pd.isna(row.get("cast")):
        continue
    try:
        for p in json.loads(row["cast"]):
            parts = p["name"].strip().split(" ", 1)
            first = parts[0] if parts[0] else "Unknown"
            last  = parts[1] if len(parts) > 1 and parts[1].strip() else " "
            eid = get_employee_id(first, last)
            if eid:
                acts_in.append({"movie_id": seq_movie_id, "employee_id": eid})
    except:
        continue
acts_in_df = pd.DataFrame(acts_in).drop_duplicates()
if len(acts_in_df) > ACTS_IN_CAP:
    acts_in_df = acts_in_df.sample(n=ACTS_IN_CAP, random_state=42)
acts_in_df.to_csv("../data/out/acts_in.csv", index=False)
print(f"acts_in.csv done - {len(acts_in_df):,} rows")

# ────────────── directs.csv ──────────────
directs = []
for _, row in merged.iterrows():
    seq_movie_id = movie_id_map.get(int(row["movieId"]))
    if seq_movie_id is None or pd.isna(row.get("crew")):
        continue
    try:
        for p in json.loads(row["crew"]):
            if p.get("job") == "Director":
                parts = p["name"].strip().split(" ", 1)
                first = parts[0] if parts[0] else "Unknown"
                last  = parts[1] if len(parts) > 1 and parts[1].strip() else " "
                eid = get_employee_id(first, last)
                if eid:
                    directs.append({"movie_id": seq_movie_id, "employee_id": eid})
    except:
        continue
directs_df = pd.DataFrame(directs).drop_duplicates()
if len(directs_df) > DIRECTS_CAP:
    directs_df = directs_df.sample(n=DIRECTS_CAP, random_state=42)
directs_df.to_csv("../data/out/directs.csv", index=False)
print(f"directs.csv done - {len(directs_df):,} rows")

# ────────────── produces.csv ──────────────
produces = []
for _, row in merged.iterrows():
    seq_movie_id = movie_id_map.get(int(row["movieId"]))
    if seq_movie_id is None or pd.isna(row.get("production_companies")):
        continue
    try:
        for c in json.loads(row["production_companies"]):
            sid = studio_id_map.get(c["name"])
            if sid:
                produces.append({"movie_id": seq_movie_id, "studio_id": sid})
    except:
        continue
produces_df = pd.DataFrame(produces).drop_duplicates()
if len(produces_df) > PRODUCES_CAP:
    produces_df = produces_df.sample(n=PRODUCES_CAP, random_state=42)
produces_df.to_csv("../data/out/produces.csv", index=False)
print(f"produces.csv done - {len(produces_df):,} rows")

# ────────────── platform.csv + has_platform.csv (no id col - SERIAL assigns) ──────────────
platforms    = ["Netflix", "Hulu", "Amazon Prime", "Disney+", "HBO Max"]
platform_ids = {name: i + 1 for i, name in enumerate(platforms)}

pd.DataFrame([{"platform_name": name} for name in platforms]) \
    .to_csv("../data/out/platform.csv", index=False)

seq_movie_ids_list = list(valid_seq_movie_ids)
has_platform = []
for seq_mid in seq_movie_ids_list:
    assigned = random.sample(platforms, k=random.randint(1, 3))
    for p in assigned:
        has_platform.append({
            "movie_id":     seq_mid,
            "platform_id":  platform_ids[p],
            "release_date": f"{random.randint(2010, 2023)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        })

has_platform_df = pd.DataFrame(has_platform).drop_duplicates(subset=["movie_id", "platform_id"])
has_platform_df.to_csv("../data/out/has_platform.csv", index=False)
print(f"has_platform.csv + platform.csv done - {len(has_platform_df):,} rows")

print("\nAll pass-2 CSVs written to data/out/")