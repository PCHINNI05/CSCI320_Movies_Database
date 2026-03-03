# CSCI 320 — Movie Streaming Database

A Netflix-style movie database application built for CSCI 320: Principles of Data Management at RIT.

**Team:** Nicholas Li · Praneel Chinni · Samuel Stewart · Ibtehaz Rafid · Sam (backend/DB lead)

---

## Project Overview

A full-featured movie streaming database supporting user accounts, collections, social following, movie search, ratings, and watch history. Built on PostgreSQL with a Java CLI frontend using raw JDBC (no ORM).

**Tech Stack**
- Java 17 + Maven
- PostgreSQL (hosted on `starbug.cs.rit.edu`)
- JDBC (PostgreSQL driver 42.7.3)
- DataGrip for DB management
- Python + psycopg2 for data loading (dev only)

---

## Project Structure

```
CSCI320_MOVIES_DATABASE/
├── sql/
│   ├── create_tables.sql       # Full DDL — run this first
│   └── load_data.sql           # Seed/bulk insert scripts
├── src/main/java/com/moviedb/
│   ├── App.java                # Entry point / main menu
│   ├── DatabaseConnection.java # Singleton JDBC connection
│   └── dao/                    # One DAO class per entity/feature
├── pom.xml
└── README.md
```

---

## Database

**Server:** `starbug.cs.rit.edu` (PostgreSQL, port 5432)  
**Access:** SSH tunnel required — use DataGrip with SSH config pointing to `starbug.cs.rit.edu:22`

### Schema (16 Tables)

**Entities:** `users`, `movie`, `collection`, `platform`, `genre`, `studio`, `employee`

**Relationships (junction tables):** `follows`, `watches`, `rates`, `collection_contents`, `has_platform`, `produces`, `has_genre`, `acts_in`, `directs`

Key constraints:
- `mpaa_rating` — enforced via `CHECK IN ('G','PG','PG-13','R','NC-17','NR')`
- `star_rating` — enforced via `CHECK BETWEEN 1 AND 5`
- `watches` PK is `(user_id, movie_id, start_time)` to allow re-watches
- `follows` has `CHECK (follower_id <> followee_id)` to prevent self-follow

---

## Local Setup

### 1. Clone & Configure Credentials

```bash
git clone <repo-url>
cd CSCI320_MOVIES_DATABASE
```

Create `src/main/resources/db.properties` — **this file is gitignored, never commit it:**

```properties
db.url=jdbc:postgresql://localhost:5432/YOUR_CS_USERNAME
db.user=YOUR_CS_USERNAME
db.password=YOUR_CS_PASSWORD
```

### 2. DataGrip Connection

1. New Data Source → PostgreSQL
2. SSH/SSL tab → Use SSH tunnel → configure:
   - Host: `starbug.cs.rit.edu` · Port: `22`
   - Auth: Password (your CS credentials)
3. General tab → Host: `localhost` · Port: `5432` · DB/User: your CS username

### 3. Build & Run

```bash
mvn clean package
java -jar target/CSCI320_MOVIES_DATABASE-1.0-SNAPSHOT.jar
```

---

## Git Conventions

### Branch Naming

| Prefix | Purpose | Example |
|--------|---------|---------|
| `db/` | Schema, DDL, seed data | `db/schema-setup` |
| `feature/` | Application features | `feature/auth` |
| `fix/` | Bug fixes | `fix/collection-delete` |

**Never commit directly to `main`.** All work goes through branches and is merged via pull request.

### Commit Style

```
<type>: short description

Examples:
db: add create_tables.sql with all 16 tables
feature: implement user login and registration
fix: correct watches PK to allow re-watches
```

### Workflow

```bash
git checkout -b feature/your-feature
# ... do work ...
git add .
git commit -m "feature: description"
git push origin feature/your-feature
# open PR → review → merge to main
```

---

## Development Guidelines

- **No ORMs.** All database interaction is raw SQL via JDBC `PreparedStatement`.
- **One DAO per logical area** — e.g. `UserDAO.java`, `MovieDAO.java`, `CollectionDAO.java`.
- **No `SELECT *`** — always select specific columns.
- **Use `PreparedStatement`** for all queries with user input — never string concatenation (SQL injection).
- `db.properties` is **always gitignored** — share credentials out of band with teammates.
- SQL scripts live in `sql/` and are committed to the repo. Python/ETL loading scripts also go here.

---

## Phase Roadmap

- [x] **Phase 1** — EER Diagram + Reduction to Tables
- [ ] **Phase 2** — Schema creation, data loading, full Java application
- [ ] **Phase 3** — Data analytics (Python + pandas + matplotlib)