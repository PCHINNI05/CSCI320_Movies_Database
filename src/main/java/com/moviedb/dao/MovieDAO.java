/**
 * FILE: MovieDAO.java
 *
 * DESCRIPTION:
 *   MovieDAO handles all database interactions related to movies.
 *   Provides methods to search for movies by title, release date, cast member, studio, or genre,
 *   sort search results, and display the results.
 *   Each search returns movie details including title, cast, director, length, MPAA rating,
 *   release date, and average user rating.
 *
 * AUTHORS:
 *   - Ibtehaz Rafid     (ir9269)
 *   - Samuel Stewart    (ses1251)
 *   - Praneel Chinni    (pjc8054)
 *
 * COURSE:  CSCI 320 - Principles of Data Management
 * SECTION: 02
 * TERM:    Spring 2026
 * GROUP:   #18
 */
package com.moviedb.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.moviedb.DatabaseConnection;

public class MovieDAO {

    /**
     * Represents a movie search result.
     */
    public record MovieResult(
            int movieId,
            String title,
            String cast,
            String directors,
            String studios,
            String genres,
            int lengthMinutes,
            String mpaaRating,
            String releaseDate, // "YYYY-MM-DD", may be null
            double avgRating // 0.0 means no ratings yet
            ) {

    }

    /**
     * All searches have the same JOIN, with only the WHERE clause differing
     * between search modes
     */
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

    /**
     * Default sort after a search: alpha by title, then earliest release date
     */
    private static final String GROUP_AND_ORDER = """
            GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
            ORDER BY m.title ASC, MIN(hp.release_date) ASC
            """;

    /**
     * Adds the appropriate WHERE clause to the base query in order to search
     * for movies by title (case-insensitive).
     *
     * @param title the movie title or partial title to search for
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByTitle(String title) {
        return searchMovies(BASE_QUERY + "WHERE m.title ILIKE ?\n" + GROUP_AND_ORDER, "%" + title + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to search
     * for movies by release date (year or full date).
     *
     * @param date date the release date or year prefix
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByReleaseDate(String date) {
        return searchMovies(BASE_QUERY + "GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating\n"
                + "HAVING TO_CHAR(MIN(hp.release_date), 'YYYY-MM-DD') LIKE ?\n"
                + "ORDER BY m.title ASC, MIN(hp.release_date) ASC\n", date + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to search
     * for movies by cast member name (case-insensitive).
     *
     * @param name the full or partial name of a cast member
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByCastMember(String name) {
        return searchMovies(BASE_QUERY + "WHERE TRIM(e_a.first_name || ' ' || e_a.last_name) ILIKE ?\n" + GROUP_AND_ORDER, "%" + name + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to search
     * for movies by studio (case-insensitive).
     *
     * @param studio the studio name or partial name
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByStudio(String studio) {
        return searchMovies(BASE_QUERY + "WHERE s.name ILIKE ?\n" + GROUP_AND_ORDER, "%" + studio + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to search
     * for movies by genre (case-insensitive).
     *
     * @param genre the genre name or partial name
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByGenre(String genre) {
        return searchMovies(BASE_QUERY + "WHERE g.genre_name ILIKE ?\n" + GROUP_AND_ORDER, "%" + genre + "%");
    }

    /**
     * * Sorts a list of movie results based on the specified field and order.
     *
     * If the field is null or unrecognized the sorting defaults to movie title.
     * Sorting can be set in ascending or descending order.
     *
     * @param results the list of MovieResult obects to sort
     * @param field the field to sort by ("title", "studio", "genre", "year")
     * @param ascending true for ascending order, false for descending order
     * @return
     */
    public List<MovieResult> sort(List<MovieResult> results, String field, boolean ascending) {
        Comparator<MovieResult> cheeseburger;
        if (field == null || field.equalsIgnoreCase("title")) {
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return a.title().toLowerCase().compareTo((b.title().toLowerCase()));
                }
            };
        } else if (field.equalsIgnoreCase("studio")) {
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return nullSafe(a.studios()).compareTo((nullSafe(b.studios())));
                }
            };
        } else if (field.equalsIgnoreCase("genre")) {
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return nullSafe(a.genres()).compareTo((nullSafe(b.genres())));
                }
            };
        } else if (field.equalsIgnoreCase("year")) {
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return nullSafe(a.releaseDate()).compareTo((nullSafe(b.releaseDate())));
                }
            };
        } else {
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return a.title().toLowerCase().compareTo(b.title().toLowerCase());
                }
            };
        }
        if (!ascending) {
            final Comparator<MovieResult> orig = cheeseburger;
            cheeseburger = new Comparator<MovieResult>() {
                public int compare(MovieResult a, MovieResult b) {
                    return -orig.compare(a, b);
                }
            };
        }
        List<MovieResult> sorted = new ArrayList<>(results);
        sorted.sort(cheeseburger);
        return sorted;
    }

    /**
     * Prints a formatted list of movie search results to the console. Displays
     * each movie with its title, cast, director, length (runtime), MPAA rating,
     * and average user rating. If no results found, a message is shown instead.
     *
     * @param results the list of MovieResult objects to display
     */
    public void printResults(List<MovieResult> results) {
        if (results.isEmpty()) {
            System.out.println("\n There's nothing in here, Shakespeare! They say a movie is worth a billion words, so you can definitely broaden your search.");
            return;
        }
        System.out.printf("%n Found %d movie(s):%n", results.size());
        for (int i = 0; i < results.size(); i++) {
            MovieResult r = results.get(i);
            String year;
            if (r.releaseDate() != null) {
                year = r.releaseDate().substring(0, 4);
            } else {
                year = "N/A";
            }
            String rating;
            if (r.avgRating() > 0) {
                rating = String.format("%.1f* (user avg)", r.avgRating());
            } else {
                rating = "no user ratings yet";
            }
            System.out.println();
            System.out.printf("  [%d] %s. (%s)%n", i + 1, r.title(), year);
            System.out.printf("      Cast:      %s%n", checkNA(r.cast()));
            System.out.printf("      Director:  %s%n", checkNA(r.directors()));
            System.out.printf("      Runtime: %d min  MPAA: %-6s  Rating: %s%n", r.lengthMinutes(), r.mpaaRating(), rating);
        }
    }

    /**
     * Executes a movie search and returns the results.
     *
     * This method prepares a SQL query, inserts the search parameter, executes
     * the query, and converts each row od the result set into a @MovieResult
     * object.
     *
     * @param sql the query to execute (must contain a single param placeholder
     * '?')
     * @param searchFor the value to bind to the query's parameter
     * @return a list of @MovieResult objects matching the search criteria empty
     * list if no results found/an error occured.
     */
    private List<MovieResult> searchMovies(String sql, String searchFor) {
        List<MovieResult> result = new ArrayList<>();
        try (var statement = DatabaseConnection.getConnection().prepareStatement(sql)) {
            statement.setString(1, searchFor);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                double average = rs.getDouble("avg_rating");
                boolean hasRating = !rs.wasNull();
                result.add(new MovieResult(
                        rs.getInt("movie_id"),
                        rs.getString("title"),
                        rs.getString("cast_members"),
                        rs.getString("directors"),
                        rs.getString("studios"),
                        rs.getString("genres"),
                        rs.getInt("length"),
                        rs.getString("mpaa_rating"),
                        rs.getString("release_date"),
                        hasRating ? average : 0.0));
            }
        } catch (Exception e) {
            System.err.println("  Search failed: " + e.getMessage());
        }
        return result;
    }

    // lets a user rate a movie from 1 to 5
    // if they already rated it before, this just updates the old rating
    public void rateMovie(Connection connect, int userID, int movieID, int starRating) throws Exception {
        if (starRating < 1 || starRating > 5) {
            throw new Exception("Rating must be between 1 and 5.");
        }

        String sql = """
            INSERT INTO rates (user_id, movie_id, star_rating)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, movie_id)
            DO UPDATE SET star_rating = EXCLUDED.star_rating
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userID);
            statement.setInt(2, movieID);
            statement.setInt(3, starRating);

            statement.executeUpdate();
        }
    }

    /**
     * Returns the top 20 trending movies based on watch activity.
     *
     * @param connect active database connection
     * @param days number of days to look back, or -1 for all time
     * @param currentUserId user id for follower filtering, or -1 for all users
     * @return list of up to 20 MovieResult objects
     *
     */
    public List<MovieResult> getTopTrending(Connection connect, int days, int currentUserId) {
        List<MovieResult> results = new ArrayList<>();

        StringBuilder sql = new StringBuilder("""
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
        JOIN watches          w   ON m.movie_id = w.movie_id
    """);

        List<Object> params = new ArrayList<>();
        List<String> conditions = new ArrayList<>();

        // Time filter
        if (days > 0) {
            conditions.add("w.start_time >= CURRENT_DATE - (? * INTERVAL '1 day')");
            params.add(days);
        }

        // Follower filter
        if (currentUserId > 0) {
            conditions.add("""
            w.user_id IN (
                SELECT f.followee_id
                FROM follows f
                WHERE f.follower_id = ?
            )
        """);
            params.add(currentUserId);
        }

        // Apply WHERE if needed
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(String.join(" AND ", conditions));
        }

        // Grouping + sorting
        sql.append("""
        GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
        ORDER BY COUNT(DISTINCT (w.user_id, w.movie_id, w.start_time)) DESC,
                 AVG(r.star_rating) DESC,
                 m.title ASC
        LIMIT 20
    """);

        try (var statement = connect.prepareStatement(sql.toString())) {

            // Bind parameters dynamically
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }

            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                double average = rs.getDouble("avg_rating");
                boolean hasRating = !rs.wasNull();

                results.add(new MovieResult(
                        rs.getInt("movie_id"),
                        rs.getString("title"),
                        rs.getString("cast_members"),
                        rs.getString("directors"),
                        rs.getString("studios"),
                        rs.getString("genres"),
                        rs.getInt("length"),
                        rs.getString("mpaa_rating"),
                        rs.getString("release_date"),
                        hasRating ? average : 0.0
                ));
            }

        } catch (Exception e) {
            System.err.println("  Trending search failed: " + e.getMessage());
        }

        printResults(results);
        return results;
    }

    /**
     * Returns the top N movies in the current calendar month
     *
     * @param connect active database connection
     * @param numReleases number of movie releases to return
     * @return list of up to 20 MovieResult objects
 *
     */
    public List<MovieResult> getTopReleasedMonth(Connection connect, int numReleases) {
        List<MovieResult> results = new ArrayList<>();

        String sql = """
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
        WHERE EXTRACT(YEAR FROM hp.release_date) = EXTRACT(YEAR FROM CURRENT_DATE)
          AND EXTRACT(MONTH FROM hp.release_date) = EXTRACT(MONTH FROM CURRENT_DATE)
        GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
        ORDER BY AVG(r.star_rating) DESC,
                 MIN(hp.release_date) ASC,
                 m.title ASC
        LIMIT ?
    """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, numReleases);

            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                double average = rs.getDouble("avg_rating");
                boolean hasRating = !rs.wasNull();

                results.add(new MovieResult(
                        rs.getInt("movie_id"),
                        rs.getString("title"),
                        rs.getString("cast_members"),
                        rs.getString("directors"),
                        rs.getString("studios"),
                        rs.getString("genres"),
                        rs.getInt("length"),
                        rs.getString("mpaa_rating"),
                        rs.getString("release_date"),
                        hasRating ? average : 0.0
                ));
            }

        } catch (Exception e) {
            System.err.println("  Monthly releases search failed: " + e.getMessage());
        }

        printResults(results);
        return results;
    }

    /**
     * Returns and prints the top 10 movies for a given user.
     * Scored by watch frequency (weighted 1.5x) + their personal star rating.
     * Movies the user has never watched won't appear here.
     *
     * @param connect active database connection
     * @param userId the user whose history we're scoring
     * @return list of up to 10 MovieResult objects
     */
    public List<MovieResult> getTopMoviesForUser(Connection connect, int userId) {
        List<MovieResult> results = new ArrayList<>();

        String sql = """
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
            JOIN watches      w   ON m.movie_id = w.movie_id  AND w.user_id = ?
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
            LEFT JOIN rates       ur  ON m.movie_id = ur.movie_id AND ur.user_id = ?
            GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
            ORDER BY (COUNT(DISTINCT w.start_time) * 1.5 + COALESCE(MAX(ur.star_rating), 0)) DESC,
                    m.title ASC
            LIMIT 10
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userId);
            statement.setInt(2, userId);

            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                double average = rs.getDouble("avg_rating");
                boolean hasRating = !rs.wasNull();
                results.add(new MovieResult(
                        rs.getInt("movie_id"),
                        rs.getString("title"),
                        rs.getString("cast_members"),
                        rs.getString("directors"),
                        rs.getString("studios"),
                        rs.getString("genres"),
                        rs.getInt("length"),
                        rs.getString("mpaa_rating"),
                        rs.getString("release_date"),
                        hasRating ? average : 0.0
                ));
            }
        } catch (Exception e) {
            System.err.println("  Top movies fetch failed: " + e.getMessage());
        }

        System.out.println("\n  Top 10 Movies:");
        if (results.isEmpty()) {
            System.out.println("  No watch history found for this user.");
        } else {
            printResults(results);
        }
        return results;
    }

    /**
     * Helper function to return a lowercase version of the string.
     *
     * @param s the input string
     * @return lowercase version of the string, or "zzz" if null
     */
    private String nullSafe(String s) {
        return s == null ? "zzz" : s.toLowerCase();
    }

    /**
     * Helper function to check if a string is null or blank and returns "N/A"
     * if so.
     *
     * @param s the input string
     * @return the original string if not null/blank, else "N/A"
     */
    private String checkNA(String s) {
        if (s == null || s.isBlank()) {
            return "N/A";
        } else {
            return s;
        }
    }

    /**
     * Movie recommendation function based on the user's and
     * similar user's activity.
     * 
     * The recommendation process:
     * 1) Find similar users who watched at least 2 of the same movies as
     *    the current user.
     * 2) Take the movies that the similar users watched and remove the ones
     *    the current user watched.
     * 3) Further refine by only returning the movies that have the
     *    same genres as the movies that the current user already watched.
     * The function prints out the result afterwards.
     * 
     * @param connect active database connection for query
     * @param userID ID of the caller to generate recommendations for
     */
    public void recommendMovies(Connection connect, int userID) {
        String sql = """
                SELECT m.movie_id, m.title
                FROM movie m
                WHERE m.movie_id IN (
                    SELECT DISTINCT w.movie_id
                    FROM watches w
                    WHERE w.user_id IN (
                        SELECT wc.user_id
                        FROM watches wb
                        JOIN watches wc ON wb.movie_id = wc.movie_id
                        WHERE wb.user_id = ?
                        AND wc.user_id != ?
                        GROUP BY wc.user_id
                        HAVING COUNT(*) >= 2
                    )
                )
                AND m.movie_id NOT IN (
                    SELECT movie_id
                    FROM watches
                    WHERE user_id = ?
                )
                AND m.movie_id IN (
                    SELECT hg.movie_id
                    FROM has_genre hg
                    WHERE hg.genre_id IN (
                        SELECT hga.genre_id
                        FROM has_genre hga, watches w
                        WHERE hga.movie_id = w.movie_id
                        AND w.user_id = ?
                    )
                )
                """;
        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, userID);
            stmt.setInt(2, userID);
            stmt.setInt(3, userID);
            stmt.setInt(4, userID);

            ResultSet rs = stmt.executeQuery();
            boolean found = false;

            while (rs.next()) {
                found = true;
                System.out.println("  [" + rs.getInt("movie_id") + "] "
                    + rs.getString("title"));
            }

            if (!found) {
                System.out.println("  No recommendations available at the moment. Try expanding your watch activity to improve recommendations");
            }
        } catch (Exception e) {
            System.err.println("  Couldn't load recommendations: " + e.getMessage());
        }
    }

}
