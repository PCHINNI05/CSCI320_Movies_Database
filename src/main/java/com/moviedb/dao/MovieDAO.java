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
 *
 * COURSE:  CSCI 320 - Principles of Data Management
 * SECTION: 02
 * TERM:    Spring 2026
 * GROUP:   #18
 */

package com.moviedb.dao;

import java.sql.Connection;
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

    /** All searches have the same JOIN, with only the WHERE clause differing between search modes */
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

    /** Default sort after a search: alpha by title, then earliest release date */
    private static final String GROUP_AND_ORDER = """
            GROUP BY m.movie_id, m.title, m.length, m.mpaa_rating
            ORDER BY m.title ASC, MIN(hp.release_date) ASC
            """;



    /**
     * Adds the appropriate WHERE clause to the base query in order to
     * search for movies by title (case-insensitive).
     * 
     * @param title the movie title or partial title to search for
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByTitle(String title) {
        return searchMovies(BASE_QUERY + "WHERE m.title ILIKE ?\n" + GROUP_AND_ORDER, "%" + title + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to
     * search for movies by release date (year or full date).
     * 
     * @param date date the release date or year prefix
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByReleaseDate(String date) {
        return searchMovies(BASE_QUERY + "WHERE TO_CHAR(hp.release_date, 'YYYY-MM-DD') LIKE ?\n" + GROUP_AND_ORDER, date +"%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to
     * search for movies by cast member name (case-insensitive).
     * 
     * @param name the full or partial name of a cast member
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByCastMember(String name) {
        return searchMovies(BASE_QUERY + "WHERE TRIM(e_a.first_name || ' ' || e_a.last_name) ILIKE ?\n" + GROUP_AND_ORDER, "%" + name + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to
     * search for movies by studio (case-insensitive).
     * 
     * @param studio the studio name or partial name
     * @return a list of matching MovieResult objects
     */
    public List<MovieResult> searchByStudio(String studio) {
        return searchMovies(BASE_QUERY + "WHERE s.name ILIKE ?\n" + GROUP_AND_ORDER, "%" + studio + "%");
    }

    /**
     * Adds the appropriate WHERE clause to the base query in order to
     * search for movies by genre (case-insensitive).
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
                    return -orig.compare(a,b);
                }
            };
        }
        List<MovieResult> sorted = new ArrayList<>(results);
        sorted.sort(cheeseburger);
        return sorted;
    }


    /**
     * Prints a formatted list of movie search results to the console.
     * Displays each movie with its title, cast, director, length (runtime),
     * MPAA rating, and average user rating.
     * If no results found, a message is shown instead.
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
            if(r.avgRating() > 0) {
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
     * This method prepares a SQL query, inserts the search parameter,
     * executes the query, and converts each row od the result set into
     * a @MovieResult object.
     * 
     * @param sql the query to execute (must contain a single param placeholder '?')
     * @param searchFor the value to bind to the query's parameter
     * @return a list of @MovieResult objects matching the search criteria
     *         empty list if no results found/an error occured.
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
                    hasRating ? average: 0.0));
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
     * Helper function to return a lowercase version of the string.
     * @param s the input string
     * @return lowercase version of the string, or "zzz" if null
     */
    private String nullSafe(String s) {
        return s == null ? "zzz" : s.toLowerCase();
    }

    /**
     * Helper function to check if a string is null
     * or blank and returns "N/A" if so.
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
}