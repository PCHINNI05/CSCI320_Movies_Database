# Poster SQL Queries

---

## Fig 1 — Average Rating by Runtime Group

Groups movies into runtime buckets and computes the average star rating for each.

```sql
SELECT
    CASE
        WHEN m.length < 90  THEN '<90'
        WHEN m.length < 110 THEN '90-109'
        WHEN m.length < 130 THEN '110-129'
        WHEN m.length < 150 THEN '130-149'
        ELSE '150+'
    END AS runtime_group,
    ROUND(AVG(r.star_rating)::numeric, 2) AS avg_rating,
    COUNT(r.star_rating)                  AS rating_count
FROM movie m
JOIN rates r ON m.movie_id = r.movie_id
GROUP BY runtime_group
ORDER BY MIN(m.length);
```

---

## Fig 2 — Average Rating by Era and Genre

Uses a CTE to classify each movie as `Older` (pre-2000) or `Newer` (2000+) based on its
earliest platform release date, then compares average ratings overall and within Sci-Fi.

```sql
WITH movie_era AS (
    SELECT
        m.movie_id,
        CASE
            WHEN MIN(hp.release_date) < '2000-01-01' THEN 'Older'
            ELSE 'Newer'
        END AS era
    FROM movie m
    JOIN has_platform hp ON m.movie_id = hp.movie_id
    GROUP BY m.movie_id
)

-- Overall (all genres)
SELECT
    'Overall' AS genre_group,
    me.era,
    ROUND(AVG(r.star_rating)::numeric, 2) AS avg_rating,
    COUNT(r.star_rating)                  AS rating_count
FROM movie m
JOIN rates     r  ON m.movie_id = r.movie_id
JOIN movie_era me ON m.movie_id = me.movie_id
GROUP BY me.era

UNION ALL

-- Sci-Fi only
SELECT
    'Sci-Fi' AS genre_group,
    me.era,
    ROUND(AVG(r.star_rating)::numeric, 2) AS avg_rating,
    COUNT(r.star_rating)                  AS rating_count
FROM movie m
JOIN rates     r  ON m.movie_id = r.movie_id
JOIN has_genre hg ON m.movie_id = hg.movie_id
JOIN genre     g  ON hg.genre_id = g.genre_id
JOIN movie_era me ON m.movie_id = me.movie_id
WHERE g.genre_name = 'Sci-Fi'
GROUP BY me.era

ORDER BY genre_group, era;
```