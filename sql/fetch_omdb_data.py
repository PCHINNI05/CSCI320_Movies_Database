import requests
import pandas as pd
import time
import json
import os
import random
import pickle

random.seed(42)

# ────────────── Config ──────────────
API_KEY       = "########"              # DO NOT, I REPEAT DO NOT PASTE API KEY AND THEN GIT COMMIT IT
REQUEST_CAP   = 100_000
SLEEP_BETWEEN = 0.05                    # 50ms between requests
PROGRESS_FILE = "../data/out/omdb_progress.json"
OUTPUT_CSV    = "../data/out/omdb_data.csv"

VALID_RATINGS = {"G", "PG", "PG-13", "R", "NC-17", "NR"}

# ────────────── Build work list ──────────────
links     = pd.read_csv("../data/ml-25m/links.csv")
links["imdbId"] = pd.to_numeric(links["imdbId"], errors="coerce")

with open("../data/out/id_maps.pkl", "rb") as f:
    maps = pickle.load(f)
movie_id_map = maps["movie_id_map"]  # orig movieId -> sequential id

rows = []
for _, row in links.iterrows():
    if pd.isna(row["imdbId"]):
        continue
    orig_id = int(row["movieId"])
    seq_id  = movie_id_map.get(orig_id)
    if seq_id is None:
        continue
    rows.append({
        "movie_id": seq_id,
        "imdb_id":  f"tt{int(row['imdbId']):07d}"
    })

work_df = pd.DataFrame(rows).drop_duplicates("movie_id")
work_df = work_df.sample(frac=1, random_state=42).reset_index(drop=True)
print(f"Total movies with IMDb IDs: {len(work_df):,}")

# ────────────── Resume support ──────────────
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        fetched = json.load(f)  # { "movie_id": { ...omdb fields... } }
    print(f"Resuming — {len(fetched):,} already fetched")
else:
    fetched = {}

def save_progress():
    with open(PROGRESS_FILE, "w") as f:
        json.dump(fetched, f)

def parse_runtime(runtime_str):
    """'81 min' -> 81, returns None if unparseable"""
    try:
        return int(runtime_str.replace("min", "").strip())
    except:
        return None

def parse_box_office(bo_str):
    """'$229,947,062' -> 229947062, returns None if unparseable"""
    try:
        return int(bo_str.replace("$", "").replace(",", "").strip())
    except:
        return None

def parse_imdb_votes(votes_str):
    """'1,163,069' -> 1163069"""
    try:
        return int(votes_str.replace(",", "").strip())
    except:
        return None

def get_rating_value(ratings_list, source):
    """Extract a specific rating source value from the Ratings array"""
    for r in ratings_list:
        if r.get("Source") == source:
            return r.get("Value")
    return None

# ────────────── Fetch ──────────────
remaining = work_df[~work_df["movie_id"].astype(str).isin(fetched.keys())]
to_fetch  = remaining.head(REQUEST_CAP - len(fetched))
print(f"Fetching {len(to_fetch):,} this run...")

for i, (_, row) in enumerate(to_fetch.iterrows()):
    try:
        r = requests.get(
            "http://www.omdbapi.com/",
            params={"i": row["imdb_id"], "apikey": API_KEY, "type": "movie"},
            timeout=10
        )
        d = r.json()

        if d.get("Response") != "True":
            fetched[str(row["movie_id"])] = None
        else:
            rated = d.get("Rated", "").strip().upper()
            ratings_list = d.get("Ratings", [])

            fetched[str(row["movie_id"])] = {
                # ── Core movie fields (can update/correct existing data) ──
                "imdb_id":       row["imdb_id"],
                "title":         d.get("Title"),
                "year":          d.get("Year"),
                "mpaa_rating":   rated if rated in VALID_RATINGS else None,
                "runtime_min":   parse_runtime(d.get("Runtime", "")),
                "released":      d.get("Released"),       # e.g. "22 Nov 1995"
                "language":      d.get("Language"),
                "country":       d.get("Country"),

                # ── People (can cross-reference / supplement existing tables) ──
                "director":      d.get("Director"),       # comma-separated
                "writer":        d.get("Writer"),
                "actors":        d.get("Actors"),         # top 3 billed

                # ── Genre ──
                "genre":         d.get("Genre"),          # comma-separated

                # ── Ratings ──
                "imdb_rating":   d.get("imdbRating"),     # e.g. "8.3"
                "imdb_votes":    parse_imdb_votes(d.get("imdbVotes", "")),
                "metascore":     d.get("Metascore"),      # e.g. "96"
                "rt_rating":     get_rating_value(ratings_list, "Rotten Tomatoes"),     # e.g. "100%"
                "mc_rating":     get_rating_value(ratings_list, "Metacritic"),          # e.g. "96/100"

                # ── Extra metadata ──
                "plot":          d.get("Plot"),
                "awards":        d.get("Awards"),
                "box_office":    parse_box_office(d.get("BoxOffice", "")),  # USD int
                "poster_url":    d.get("Poster"),
                "production":    d.get("Production"),
            }

    except Exception as e:
        print(f"  Error on movie_id {row['movie_id']} ({row['imdb_id']}): {e}")
        fetched[str(row["movie_id"])] = None

    if (i + 1) % 500 == 0:
        save_progress()
        print(f"  {i + 1:,} fetched so far...")

    time.sleep(SLEEP_BETWEEN)

save_progress()

# ────────────── Write final CSV ──────────────
out_rows = []
for movie_id, data in fetched.items():
    if data is None:
        out_rows.append({"movie_id": int(movie_id)})
    else:
        out_rows.append({"movie_id": int(movie_id), **data})

out_df = pd.DataFrame(out_rows)

filled      = out_df["mpaa_rating"].notna().sum() if "mpaa_rating" in out_df.columns else 0
total       = len(out_df)
print(f"\nDone — {total:,} fetched, {filled:,} had valid MPAA ratings ({filled/total*100:.1f}%)")

out_df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved to {OUTPUT_CSV}")
print("\nColumns saved:", list(out_df.columns))