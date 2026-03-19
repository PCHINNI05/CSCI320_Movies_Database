package com.moviedb.dao;

import java.sql.Connection;

public class MovieDAO {

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
}