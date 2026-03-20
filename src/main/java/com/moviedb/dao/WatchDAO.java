package com.moviedb.dao;

import java.sql.Connection;

public class WatchDAO {

    // marks one movie as watched by the current user
    // start time is now, and end time is based on the movie length
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

    // Plays every movie in a collection. Inserts a "watches" row for each one
    public void watchCollection(Connection connect, int userID, int collectionID) throws Exception {
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