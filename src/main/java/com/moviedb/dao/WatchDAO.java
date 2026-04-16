/**
 * FILE: WatchDAO.java
 *
 * DESCRIPTION:
 *   Data access object for recording movie watch events.
 *   Inserts rows into the watches table when a user plays a single movie or
 *   an entire collection. Start time is always the current timestamp and
 *   end time is derived from each movie's stored runtime.
 *
 * AUTHORS:
 *   - Ibtehaz Rafid     (ir9269)
 *   - Samuel Stewart    (ses1251)
 *   - Nicholas Lim      (nl8228)
 * 
 * COURSE:  CSCI 320 - Principles of Data Management
 * SECTION: 02
 * TERM:    Spring 2026
 * GROUP:   #18
 */
package com.moviedb.dao;

import java.sql.Connection;

/**
 * Data access object for recording movie watch events.
 * <p>
 * Inserts rows into the {@code watches} table when a user plays a movie or an
 * entire collection. {@code start_time} is always set to the current timestamp
 * and {@code end_time} is derived from the movie's stored runtime.
 */
public class WatchDAO {

    /**
     * Records a single movie as watched by the given user.
     * <p>
     * Inserts one row into {@code watches} with {@code start_time} set to the current
     * timestamp and {@code end_time} computed as {@code start_time + runtime}.
     *
     * @param connect active database connection
     * @param userID  the ID of the user watching the movie
     * @param movieID the ID of the movie to mark as watched
     * @throws Exception if the movie ID does not exist or the insert fails
     */
    public void watchMovie(Connection connect, int userID, int movieID) throws Exception {
        String sql = """
            INSERT INTO watches (user_id, movie_id, start_time, end_time)
            SELECT
                ?,
                m.movie_id,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP + (m.length * INTERVAL '1 minute')
            FROM movie m
            WHERE m.movie_id = ?
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userID);
            statement.setInt(2, movieID);

            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Movie not found.");
            }
        }
    }

    /**
     * Records every movie in a collection as watched by the given user.
     * <p>
     * Verifies collection ownership first, then inserts one {@code watches} row
     * per movie in a single bulk statement.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user playing the collection
     * @param collectionID the ID of the collection to play
     * @throws Exception if the collection does not belong to the user, the collection
     *                   is empty, or an insert fails
     */
    public void watchCollection(Connection connect, int userID, int collectionID) throws Exception {
        // ownership check
        String check = "SELECT 1 FROM collection WHERE collection_id = ? AND user_id = ?";
        try (var s = connect.prepareStatement(check)) {
            s.setInt(1, collectionID); s.setInt(2, userID);
            if (!s.executeQuery().next()) throw new Exception("Collection not found or not yours.");
        }

        String sql = """
            INSERT INTO watches (user_id, movie_id, start_time, end_time)
            SELECT
                ?,
                m.movie_id,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP + (m.length * INTERVAL '1 minute')
            FROM collection_contents cc
            JOIN movie m ON cc.movie_id = m.movie_id
            WHERE cc.collection_id = ?
            """;
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userID);
            statement.setInt(2, collectionID);
            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Collection is empty or not found.");
            }
        }
    }
}