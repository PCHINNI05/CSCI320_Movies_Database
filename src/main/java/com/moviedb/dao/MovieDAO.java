package com.moviedb.dao;

import com.moviedb.DatabaseConnection;

import java.sql.*;
import java.util.*;

public class MovieDAO {

    public record MovieResult(
            int    movieId,
            String title,
            String cast,
            String directors,
            String studios,
            String genres,
            int    lengthMinutes,
            String mpaaRating,
            String releaseDate,   // "YYYY-MM-DD", may be null
            double avgRating      // 0.0 means no ratings yet
    ) {}

    // All searches share the same big JOIN — only the WHERE clause differs
    private static final String BASE_QUERY = """
            SELECT
                m.movie_id,
                m.title,
                m.length,
                m.mpaa_rating,
                STRING_AGG(DISTINCT TRIM(e_a.first_name || ' ' || e_a.last_name), ', '
                           ORDER BY TRIM(e_a.first_name || ' ' || e_a.last_name)) AS cast_members,
                STRING_AGG(DISTINCT TRIM(e_d.first_name || ' ' || e_d.last_name), ', ') AS directors,
                STRING_AGG(DISTINCT s.name, ', ')        AS studios,
                STRING_AGG(DISTINCT g.genre_name, ', ')  AS genres,
                TO_CHAR(MIN(hp.release_date), 'YYYY-MM-DD') AS release_date,
                ROUND(AVG(r.star_rating)::numeric, 1)    AS avg_rating
            FROM movie m
            LEFT JOIN acts_in     ai  ON m.movie_id = ai.movie_id
            LEFT JOIN employee    e_a ON ai.employee_id = e_a.employee_id
            LEFT JOIN directs     d   ON m.movie_id = d.movie_id
            LEFT JOIN employee    e_d ON d.employee_id = e_d.employee_id
            LEFT JOIN produces    p   ON m.movie_id = p.movie_id
            LEFT JOIN studio      s   ON p.studio_id = s.studio_id
            LEFT JOIN has_genre   hg  ON m.movie_id = hg.movie_id
            LEFT JOIN genre       g   ON hg.genre_id = g.genre_id
            LEFT JOIN has_platform hp ON m.movie_id = hp.movie_id
            LEFT JOIN rates       r   ON m.movie_id = r.movie_id
            """;

    // Default sort after a fresh search: alpha by title, then earliest release date
    private static final String GROUP_AND_ORDER = """
            GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
            ORDER BY m.title ASC, MIN(hp.release_date) ASC
            """;

    // ---------- Search ----------

    public List<MovieResult> searchByTitle(String title) {
        return List.of();
    }

    public List<MovieResult> searchByReleaseDate(String date) {
        return List.of();
    }

    public List<MovieResult> searchByCastMember(String name) {
        return List.of();
    }

    public List<MovieResult> searchByStudio(String studio) {
        return List.of();
    }

    public List<MovieResult> searchByGenre(String genre) {
        return List.of();
    }

    // ---------- Sort ----------

    /** Field options: "title", "studio", "genre", "year" */
    public List<MovieResult> sort(List<MovieResult> results, String field, boolean ascending) {
        return results;
    }

    // ---------- Display ----------

    public void printResults(List<MovieResult> results) {}

    // ---------- Private helpers ----------

    private List<MovieResult> runSearch(String sql, String param) {
        return List.of();
    }

    /** Null-safe lower-case string for sorting — nulls sort to the end. */
    private String nullSafe(String s) {
        return s == null ? "zzz" : s.toLowerCase();
    }

    private String orNA(String s) {
        return (s == null || s.isBlank()) ? "N/A" : s;
    }
}