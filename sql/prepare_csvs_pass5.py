import pandas as pd
import pickle

# ────────────── Load ──────────────
omdb_df = pd.read_csv("../data/out/omdb_data.csv")[["movie_id", "director", "actors"]]
omdb_df["movie_id"] = omdb_df["movie_id"].astype(int)

# Load existing employee map
with open("../data/out/id_maps.pkl", "rb") as f:
    maps = pickle.load(f)
employee_id_map = maps["employee_id_map"]  # (first, last) -> seq id

# Load existing employee.csv to know current count (new IDs continue from here)
emp_df = pd.read_csv("../data/out/employee.csv")
next_emp_id = len(emp_df) + 1  # SERIAL will assign this next

# Load existing directs/acts_in to merge with
existing_directs  = pd.read_csv("../data/out/directs.csv")
existing_acts_in  = pd.read_csv("../data/out/acts_in.csv")

# ────────────── Helper ──────────────
def parse_name(full_name):
    """'John Lasseter' -> ('John', 'Lasseter'), single names get ' ' as last"""
    parts = full_name.strip().split(" ", 1)
    first = parts[0] if parts[0] else "Unknown"
    last  = parts[1] if len(parts) > 1 and parts[1].strip() else " "
    return first, last

def get_or_add_employee(first, last):
    """Return existing seq id or add to employee list and return new id."""
    global next_emp_id
    key = (first, last)
    if key in employee_id_map:
        return employee_id_map[key]
    # New employee - assign next sequential id
    employee_id_map[key] = next_emp_id
    new_employees.append({"first_name": first, "last_name": last})
    next_emp_id += 1
    return employee_id_map[key]

# ────────────── Parse OMDb directors + actors ──────────────
new_employees = []
new_directs   = []
new_acts_in   = []

for _, row in omdb_df.iterrows():
    movie_id = int(row["movie_id"])

    # Directors
    if pd.notna(row.get("director")) and str(row["director"]).strip() not in ("N/A", ""):
        for name in str(row["director"]).split(","):
            first, last = parse_name(name)
            eid = get_or_add_employee(first, last)
            new_directs.append({"movie_id": movie_id, "employee_id": eid})

    # Actors (OMDb gives top 3 billed)
    if pd.notna(row.get("actors")) and str(row["actors"]).strip() not in ("N/A", ""):
        for name in str(row["actors"]).split(","):
            first, last = parse_name(name)
            eid = get_or_add_employee(first, last)
            new_acts_in.append({"movie_id": movie_id, "employee_id": eid})

# ────────────── Merge and deduplicate ──────────────
directs_df = pd.concat([existing_directs, pd.DataFrame(new_directs)], ignore_index=True) \
               .drop_duplicates()
acts_in_df = pd.concat([existing_acts_in, pd.DataFrame(new_acts_in)], ignore_index=True) \
               .drop_duplicates()

# ────────────── Save ──────────────
# Append new employees to employee.csv (no id col - SERIAL continues from next_emp_id)
if new_employees:
    new_emp_df = pd.DataFrame(new_employees)
    new_emp_df["first_name"] = new_emp_df["first_name"].fillna("Unknown")
    new_emp_df["last_name"]  = new_emp_df["last_name"].fillna(" ").replace("", " ")
    combined_emp = pd.concat([emp_df, new_emp_df], ignore_index=True)
    combined_emp.to_csv("../data/out/employee.csv", index=False)
    print(f"employee.csv updated - {len(new_employees):,} new employees added ({len(combined_emp):,} total)")
else:
    print("employee.csv unchanged - no new employees found")

directs_df.to_csv("../data/out/directs.csv", index=False)
acts_in_df.to_csv("../data/out/acts_in.csv", index=False)
print(f"directs.csv  - {len(existing_directs):,} → {len(directs_df):,} rows")
print(f"acts_in.csv  - {len(existing_acts_in):,} → {len(acts_in_df):,} rows")

# Update id_maps.pkl with new employees
maps["employee_id_map"] = employee_id_map
with open("../data/out/id_maps.pkl", "wb") as f:
    pickle.dump(maps, f)
print("id_maps.pkl updated")