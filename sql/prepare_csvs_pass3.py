import pandas as pd
import random
import pickle
from datetime import datetime, timedelta

random.seed(42)

# ────────────── Config ──────────────
NUM_COLLECTIONS            = 5_000
COLLECTION_CONTENTS_TARGET = 60_000
FOLLOWS_TARGET             = 50_000
WATCHES_TARGET             = 80_000

BASE_DATE = datetime(2015, 1, 1)

COLLECTION_NAME_POOL = [
    "My Favorites", "Watch Later", "Action Picks", "Comedy Night", "Date Night",
    "Classics", "Sci-Fi Marathon", "Horror Fest", "Family Time", "Award Winners",
    "Hidden Gems", "Top Rated", "Weekend Binge", "Must Watch", "Old Favorites",
    "New Releases", "Best of the Decade", "Director Showcase", "Cult Classics",
    "Animated Greats", "Documentaries", "Foreign Films", "Thriller Night",
    "Oscar Winners", "Rainy Day Picks", "Feel-Good Movies", "Mind-Benders",

    "Late Night Movies", "Sunday Chill", "Blockbuster Hits", "All-Time Favorites",
    "Underrated Picks", "Critically Acclaimed", "Epic Adventures", "Tearjerkers",
    "Laugh Out Loud", "Edge of Your Seat", "Nostalgia Trip", "Road Trip Movies",
    "Coming of Age", "Dark Thrillers", "Classic Hollywood", "Modern Masterpieces",
    "Indie Darlings", "Festival Favorites", "Award Season", "Director’s Cuts",
    "Best Picture Winners", "Golden Age Cinema", "Retro Rewinds", "Throwback Night",
    "Binge Worthy", "Movie Night Essentials", "Comfort Movies", "Feel Good Favorites",

    "Spooky Season", "Halloween Marathon", "Christmas Classics", "Holiday Movies",
    "Summer Blockbusters", "Winter Nights", "Spring Picks", "Autumn Vibes",
    "Beach Day Movies", "Snow Day Cinema", "Rainy Afternoon", "Lazy Sunday",

    "Action Marathon", "Explosive Action", "Martial Arts Madness", "Spy Thrillers",
    "Superhero Saga", "Fantasy Worlds", "Epic Battles", "Adventure Time",
    "Space Adventures", "Alien Encounters", "Time Travel Tales", "Cyberpunk Stories",

    "Psychological Thrillers", "Mystery Night", "Whodunits", "Crime Stories",
    "Heist Movies", "Detective Stories", "Gangster Classics", "True Crime Films",
    "Courtroom Drama", "Police Stories", "Underworld Tales",

    "Romantic Classics", "Rom-Com Favorites", "Love Stories", "Heartfelt Romance",
    "Breakup Movies", "Hopeless Romantics", "Date Night Picks", "Romantic Dramas",

    "Family Adventures", "Kids Favorites", "Animated Magic", "Pixar Style Picks",
    "Cartoon Classics", "Family Movie Night", "Magical Worlds",

    "Musical Movies", "Dance Films", "Concert Films", "Music Legends",
    "Rock and Roll Movies", "Broadway on Screen",

    "Sports Stories", "Underdog Tales", "Champion Stories", "Team Spirit",
    "Olympic Stories", "Game Day Movies",

    "War Epics", "Historical Dramas", "Period Pieces", "Ancient Worlds",
    "Medieval Tales", "Biographical Films", "True Story Movies",

    "Philosophical Films", "Thought Provoking", "Deep Cuts", "Art House Picks",
    "Experimental Cinema", "Visual Masterpieces", "Slow Cinema",

    "International Cinema", "European Classics", "Asian Cinema",
    "Latin American Stories", "Global Cinema", "World Movie Tour",

    "Director Spotlight", "Auteur Collection", "Visionary Filmmakers",
    "Cinematography Showcase", "Screenwriting Greats",

    "Cult Favorites", "Midnight Movies", "So Bad It’s Good",
    "Camp Classics", "B-Movie Madness",

    "Zombie Apocalypse", "Vampire Stories", "Monster Movies",
    "Ghost Stories", "Haunted Nights", "Creature Features",

    "Post-Apocalyptic Worlds", "Dystopian Futures", "Survival Stories",
    "End of the World", "Disaster Movies",

    "Tech and AI", "Virtual Reality", "Future Worlds",
    "Robots and Machines", "Digital Nightmares",

    "Travel the World", "City Stories", "Small Town Tales",
    "Island Adventures", "Desert Journeys",

    "Friendship Stories", "Family Drama", "Life Lessons",
    "Growing Up Stories", "Second Chances",

    "Chill Background Movies", "Easy Watching", "Lighthearted Picks",
    "Weekend Relaxation", "Late Night Comfort",

    "Study Break Movies", "After Finals", "Dorm Room Picks",
    "College Favorites", "Procrastination Station",

    "Top IMDb Picks", "Critics Choice", "Fan Favorites",
    "Most Rewatched", "All-Time Hits",

    "Short and Sweet", "Epic Length Films", "Three Hour Epics",
    "Quick Watch", "Long Haul Cinema",

    "Mind Twisters", "Plot Twist Movies", "Unreliable Narrators",
    "Puzzle Movies", "Reality Benders",

    "Feel the Adrenaline", "High Stakes", "Intense Drama",
    "Emotional Rollercoaster", "Heart Racing",

    "Quiet Masterpieces", "Minimalist Cinema",
    "Atmospheric Films", "Moody Movies",

    "Saturday Night Movies", "Friday Night Picks",
    "Midweek Escape", "After Work Cinema",

    "Ultimate Movie Marathon", "Mega Movie Night",
    "The Long Watchlist", "Cinema Essentials",
]

# ────────────── Load ID maps from pass1 ──────────────
with open("../data/out/id_maps.pkl", "rb") as f:
    maps = pickle.load(f)

user_id_map  = maps["user_id_map"]   # orig userId  --> sequential id
movie_id_map = maps["movie_id_map"]  # orig movieId --> sequential id

seq_user_ids  = sorted(user_id_map.values())
seq_movie_ids = sorted(movie_id_map.values())

# ────────────── Load movie lengths for realistic watch times ──────────────
movie_df     = pd.read_csv("../data/out/movie.csv")
# movie.csv has no id col - row index + 1 is the sequential id
movie_df["movie_id"] = range(1, len(movie_df) + 1)
movie_lengths = dict(zip(movie_df["movie_id"], movie_df["length"]))

# ────────────── collection.csv (no id col - SERIAL assigns) ──────────────
collections = []
name_counters = {}
for _ in range(NUM_COLLECTIONS):
    uid     = random.choice(seq_user_ids)
    base    = random.choice(COLLECTION_NAME_POOL)
    name_counters[base] = name_counters.get(base, 0) + 1
    name    = base if name_counters[base] == 1 else f"{base} #{name_counters[base]}"
    created = BASE_DATE + timedelta(days=random.randint(0, 3000),
                                    hours=random.randint(0, 23),
                                    minutes=random.randint(0, 59))
    collections.append({
        "collection_name": name[:200],
        "user_id":         uid,
        "creation_date":   created.strftime("%Y-%m-%d %H:%M:%S")
    })

collection_df = pd.DataFrame(collections)
collection_df.to_csv("../data/out/collection.csv", index=False)
# Sequential collection id = row index + 1 (matches SERIAL insert order)
collection_seq_ids = list(range(1, NUM_COLLECTIONS + 1))
print(f"collection.csv done — {len(collection_df):,} rows")

# ────────────── collection_contents.csv ──────────────
collection_contents = set()
for cid in collection_seq_ids:
    for _ in range(random.randint(5, 20)):
        collection_contents.add((cid, random.choice(seq_movie_ids)))
while len(collection_contents) < COLLECTION_CONTENTS_TARGET:
    collection_contents.add((random.choice(collection_seq_ids), random.choice(seq_movie_ids)))

cc_df = pd.DataFrame(list(collection_contents), columns=["collection_id", "movie_id"])
cc_df.to_csv("../data/out/collection_contents.csv", index=False)
print(f"collection_contents.csv done - {len(cc_df):,} rows")

# ────────────── follows.csv ──────────────
follows = set()
for uid in seq_user_ids:
    candidates = random.sample(seq_user_ids, min(random.randint(3, 8), len(seq_user_ids)))
    for fid in candidates:
        if fid != uid:
            follows.add((uid, fid))
while len(follows) < FOLLOWS_TARGET:
    a = random.choice(seq_user_ids)
    b = random.choice(seq_user_ids)
    if a != b:
        follows.add((a, b))

follows_df = pd.DataFrame(list(follows), columns=["follower_id", "followee_id"])
follows_df.to_csv("../data/out/follows.csv", index=False)
print(f"follows.csv done - {len(follows_df):,} rows")

# ────────────── watches.csv ──────────────
watches = []
for _ in range(WATCHES_TARGET):
    uid    = random.choice(seq_user_ids)
    mid    = random.choice(seq_movie_ids)
    start  = BASE_DATE + timedelta(
        days=random.randint(0, 3000),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    length = movie_lengths.get(mid, 90)
    end = (start + timedelta(minutes=length)
           if random.random() < 0.8
           else start + timedelta(minutes=random.randint(5, max(6, length - 1))))
    watches.append({
        "user_id":    uid,
        "movie_id":   mid,
        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time":   end.strftime("%Y-%m-%d %H:%M:%S")
    })

watches_df = pd.DataFrame(watches).drop_duplicates(subset=["user_id", "movie_id", "start_time"])
watches_df.to_csv("../data/out/watches.csv", index=False)
print(f"watches.csv done - {len(watches_df):,} rows")

print("\nAll pass-3 CSVs written to data/out/")