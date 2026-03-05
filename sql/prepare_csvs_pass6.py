import pandas as pd
import random

random.seed(42)

# ── movie.csv ──
movie_df = pd.read_csv("../data/out/movie.csv")
movie_df["mpaa_rating"] = movie_df["mpaa_rating"].fillna("NR")
movie_df.to_csv("../data/out/movie.csv", index=False)
print(f"movie: {movie_df['mpaa_rating'].eq('NR').sum():,} rows set to NR")

# ── users.csv ──
user_df = pd.read_csv("../data/out/users.csv")

user_df["creation_date"] = [
    f"{random.randint(2010, 2020)}-{random.randint(1,12):02d}-{random.randint(1,28):02d} "
    f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    for _ in range(len(user_df))
]

user_df["creation_date"] = pd.to_datetime(user_df["creation_date"])

user_df["last_access_date"] = user_df["creation_date"].apply(
    lambda cd: cd + pd.Timedelta(days=random.randint(0, 365 * 4),
                                  hours=random.randint(0, 23),
                                  minutes=random.randint(0, 59),
                                  seconds=random.randint(0, 59))
)

user_df["creation_date"]    = user_df["creation_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
user_df["last_access_date"] = user_df["last_access_date"].dt.strftime("%Y-%m-%d %H:%M:%S")

user_df.to_csv("../data/out/users.csv", index=False)
print(f"user: creation_date + last_access_date added/filled for {len(user_df):,} rows")