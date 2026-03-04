import pandas as pd
import random
import pickle

random.seed(42)

# ────────────── Load ──────────────
movie_df = pd.read_csv("../data/out/movie.csv")
omdb_df  = pd.read_csv("../data/out/omdb_data.csv")

# Temporarily add sequential movie_id for joining (matches SERIAL insert order)
movie_df.insert(0, "movie_id", range(1, len(movie_df) + 1))

omdb_slim = omdb_df[["movie_id", "mpaa_rating", "runtime_min", "title",
                      "year", "imdb_votes"]].copy()
omdb_slim["movie_id"] = omdb_slim["movie_id"].astype(int)

merged = movie_df.merge(omdb_slim, on="movie_id", how="left")

# ────────────── Update movie.csv fields ──────────────
# mpaa_rating
merged["mpaa_rating"] = merged["mpaa_rating_y"].fillna(merged["mpaa_rating_x"])

# length — only replace if OMDb has a valid value
merged["length"] = merged["runtime_min"].where(
    merged["runtime_min"].notna() & (merged["runtime_min"] > 0),
    merged["length"]
).astype(int)

# title — use OMDb clean title (no year suffix) where available
merged["title"] = merged["title_y"].where(
    merged["title_y"].notna(),
    merged["title_x"]
)

# Parse release year — prefer OMDb year, fall back to parsing MovieLens title "(YYYY)"
def extract_year(omdb_year, ml_title):
    try:
        y = int(str(omdb_year).strip()[:4])
        if 1888 <= y <= 2025:
            return y
    except:
        pass
    try:
        # MovieLens titles often end with " (1995)"
        y = int(str(ml_title).strip()[-5:-1])
        if 1888 <= y <= 2025:
            return y
    except:
        pass
    return None

merged["release_year"] = merged.apply(
    lambda r: extract_year(r["year"], r["title_x"]), axis=1
)

# Save updated movie.csv (drop all temp cols, keep no movie_id — SERIAL assigns)
movie_out = merged[["title", "length", "mpaa_rating"]].copy()
movie_out.to_csv("../data/out/movie.csv", index=False)
print(f"movie.csv updated")
print(f"  mpaa_rating filled : {movie_out['mpaa_rating'].notna().sum():,}")
print(f"  length from OMDb   : {merged['runtime_min'].notna().sum():,}")
print(f"  titles from OMDb   : {merged['title_y'].notna().sum():,}")

# ────────────── Platform definitions ──────────────
# No id col — SERIAL assigns 1..8 in this exact order
PLATFORMS = [
    # (name,          available_from_year)
    ("Theater",       1888),
    ("DVD",           1997),
    ("Blu-ray",       2006),
    ("Netflix",       2007),
    ("Hulu",          2008),
    ("Amazon Prime",  2011),
    ("Disney+",       2019),
    ("HBO Max",       2020),
]
PLATFORM_IDS   = {name: i + 1 for i, (name, _) in enumerate(PLATFORMS)}
PLATFORM_START = {name: yr   for name, yr in PLATFORMS}

pd.DataFrame([{"platform_name": name} for name, _ in PLATFORMS]) \
    .to_csv("../data/out/platform.csv", index=False)
print(f"\nplatform.csv written — {len(PLATFORMS)} platforms")

STREAMING = ["Netflix", "Hulu", "Amazon Prime", "Disney+", "HBO Max"]

def rand_date(year, month_min=1, month_max=12):
    """Return a YYYY-MM-DD string for a random day in the given year/month range."""
    m = random.randint(month_min, month_max)
    d = random.randint(1, 28)
    return f"{year}-{m:02d}-{d:02d}"

def months_later(base_year, base_month, add_months):
    total = (base_year - 1) * 12 + (base_month - 1) + add_months
    return int(total // 12 + 1), int(total % 12 + 1)

# ────────────── Generate has_platform.csv ──────────────
has_platform = []

for _, row in merged.iterrows():
    seq_mid      = int(row["movie_id"])
    year         = row["release_year"]
    year = int(year) if year is not None and not pd.isna(year) else None
    imdb_votes   = row["imdb_votes"]
    entries      = {}   # platform_id -> release_date (dedupe)

    # ── Determine first release type ──
    if year is None:
        # No year info — just assign random streaming platforms
        for p in random.sample(STREAMING, k=random.randint(1, 3)):
            entries[PLATFORM_IDS[p]] = rand_date(random.randint(2010, 2023))
        for pid, dt in entries.items():
            has_platform.append({"movie_id": seq_mid, "platform_id": pid, "release_date": dt})
        continue

    low_profile = (
        year >= 1997
        and (pd.isna(imdb_votes) or int(imdb_votes) < 1000)
        and random.random() < 0.70
    )

    if low_profile:
        # First release: DVD (if year >= 1997) or streaming (if year >= 2007)
        options = []
        if year >= 1997:
            options.append("DVD")
        if year >= 2007:
            options += [p for p in STREAMING if year >= PLATFORM_START[p]]
        if not options:
            options = ["DVD"]
        first = random.choice(options)
        first_month = random.randint(1, 12)
        entries[PLATFORM_IDS[first]] = rand_date(year, first_month, first_month)
    else:
        # Theater release in release year
        entries[PLATFORM_IDS["Theater"]] = rand_date(year)
        theater_month = random.randint(1, 12)

    # ── DVD release ──
    # Typically 3–6 months after theater; from 1997 onward
    if year >= 1997 and PLATFORM_IDS["DVD"] not in entries:
        if low_profile:
            dvd_year = year
            dvd_month = random.randint(1, 12)
        else:
            dvd_year, dvd_month = months_later(year, theater_month, random.randint(3, 6))
        if dvd_year <= 2023:
            entries[PLATFORM_IDS["DVD"]] = f"{dvd_year}-{dvd_month:02d}-{random.randint(1,28):02d}"

    # ── Blu-ray release ──
    # From 2006; same window as DVD for movies released 2006+, otherwise a later re-release
    if year >= 2006:
        if low_profile:
            br_year  = year
            br_month = random.randint(1, 12)
        else:
            br_year, br_month = months_later(year, theater_month, random.randint(3, 8))
        if br_year <= 2023:
            entries[PLATFORM_IDS["Blu-ray"]] = f"{br_year}-{br_month:02d}-{random.randint(1,28):02d}"
    elif year >= 1888:
        # Older classic — chance of a Blu-ray remaster release
        if random.random() < 0.30:
            br_year = random.randint(2008, 2020)
            entries[PLATFORM_IDS["Blu-ray"]] = rand_date(br_year)

    # ── Streaming releases ──
    # Each streaming platform became available in different years; movies land on them
    # some time after theatrical/home video, weighted toward more recent windows
    for platform in STREAMING:
        platform_launch = PLATFORM_START[platform]
        earliest_stream = max(platform_launch, int(year) + 1)
        if earliest_stream > 2024:
            continue
        # ~60% chance of being on each streaming platform
        if random.random() < 0.60:
            stream_year = random.randint(earliest_stream, min(2023, earliest_stream + 8))
            entries[PLATFORM_IDS[platform]] = rand_date(stream_year)

    # Must have at least one platform
    if not entries:
        fallback_pid = PLATFORM_IDS["DVD"] if year >= 1997 else PLATFORM_IDS["Theater"]
        entries[fallback_pid] = rand_date(year if year else 2000)

    for pid, dt in entries.items():
        has_platform.append({"movie_id": seq_mid, "platform_id": pid, "release_date": dt})

has_platform_df = pd.DataFrame(has_platform).drop_duplicates(subset=["movie_id", "platform_id"])
has_platform_df.to_csv("../data/out/has_platform.csv", index=False)
print(f"has_platform.csv written — {len(has_platform_df):,} rows")

# ────────────── Summary ──────────────
dist = has_platform_df.merge(
    pd.DataFrame([{"platform_id": i+1, "platform_name": n} for i,(n,_) in enumerate(PLATFORMS)]),
    on="platform_id"
).groupby("platform_name").size().sort_values(ascending=False)
print("\nPlatform distribution:")
for name, count in dist.items():
    print(f"  {name:<15} {count:>8,}")