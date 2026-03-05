import pandas as pd

movie_df = pd.read_csv("../data/out/movie.csv")
movie_df["mpaa_rating"] = movie_df["mpaa_rating"].fillna("NR")
movie_df.to_csv("../data/out/movie.csv", index=False)
print(f"Done - {movie_df['mpaa_rating'].eq('NR').sum():,} rows set to NR")