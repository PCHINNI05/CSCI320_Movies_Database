package com.moviedb.dao;

import java.sql.Connection;
import java.sql.ResultSet;

public class CollectionDAO {

    // makes a new collection for the current user
    public void createCollection(Connection connect, int userID, String collectionName) throws Exception {
        String sql = """
            INSERT INTO collection (collection_name, user_id)
            VALUES (?, ?)
            """;

        try (var statement = connect.prepareStatement(sql)) { statement.setString(1, collectionName);
            statement.setInt(2, userID);
            statement.executeUpdate();
        }
    }

    // lets the user change the name of one of their collections
    public void renameCollection(Connection connect, int userID, int collectionID, String newName) throws Exception {
        assertCollectionOwned(connect, userID, collectionID);

        String sql = """
            UPDATE collection
            SET collection_name = ?
            WHERE collection_id = ? AND user_id = ?
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setString(1, newName);
            statement.setInt(2, collectionID);
            statement.setInt(3, userID);

            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Collection not found.");
            }
        }
    }

    // deletes a collection and clears out its movies first so foreign key issues do not happen
    public void deleteCollection(Connection connect, int userID, int collectionID) throws Exception {
        assertCollectionOwned(connect, userID, collectionID);

        // delete child rows first in case fk is not ON DELETE CASCADE
        String deleteContents = """
            DELETE FROM collection_contents
            WHERE collection_id = ?
            """;

        try (var statement = connect.prepareStatement(deleteContents)) {
            statement.setInt(1, collectionID);
            statement.executeUpdate();
        }

        String deleteCollection = """
            DELETE FROM collection
            WHERE collection_id = ? AND user_id = ?
            """;

        try (var statement = connect.prepareStatement(deleteCollection)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, userID);

            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Collection not found.");
            }
        }
    }
    // adds a movie into one of the user's collections
    public void addMovieToCollection(Connection connect, int userID, int collectionID, int movieID) throws Exception {
        assertCollectionOwned(connect, userID, collectionID);

        String sql = """
            INSERT INTO collection_contents (collection_id, movie_id)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, movieID);
            int rows = statement.executeUpdate();
            if (rows == 0) {
                System.out.println("  That movie is already in the collection.");
            }
        }
    }

    // removes a movie from a collection the user owns
    public void removeMovieFromCollection(Connection connect, int userID, int collectionID, int movieID) throws Exception {
        assertCollectionOwned(connect, userID, collectionID);

        String sql = """
            DELETE FROM collection_contents
            WHERE collection_id = ? AND movie_id = ?
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, movieID);

            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Movie not found in collection.");
            }
        }
    }

    // shows all collections for the current user, along with movie count and total runtime
    public void getUserCollections(Connection connect, int userID) throws Exception {
        String sql = """
            SELECT
                c.collection_id,
                c.collection_name,
                COUNT(cc.movie_id) AS movie_count,
                CASE
                    WHEN SUM(m.length) IS NULL THEN 0
                    ELSE SUM(m.length)
                END AS total_minutes
            FROM collection c
            LEFT JOIN collection_contents cc
                ON c.collection_id = cc.collection_id
            LEFT JOIN movie m
                ON cc.movie_id = m.movie_id
            WHERE c.user_id = ?
            GROUP BY c.collection_id, c.collection_name
            ORDER BY c.collection_name ASC
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userID);

            try (ResultSet result = statement.executeQuery()) {
                System.out.println();
                System.out.println("  ID | Collection Name | # Movies | Total Length");
                System.out.println("  ----------------------------------------------");

                boolean foundAny = false;
                while (result.next()) {
                    foundAny = true;

                    int collectionID = result.getInt("collection_id");
                    String collectionName = result.getString("collection_name");
                    int movieCount = result.getInt("movie_count");
                    int totalMinutes = result.getInt("total_minutes");

                    System.out.printf("  %d | %s | %d | %s%n", collectionID, collectionName, movieCount, formatMinutes(totalMinutes));
                }
                if (!foundAny) {
                    System.out.println("  You do not have any collections yet.");
                }
            }
        }
    }

    // shows all the movies inside one specific collection
    public void getCollectionDetails(Connection connect, int userID, int collectionID) throws Exception {
        assertCollectionOwned(connect, userID, collectionID);

        String sql = """
            SELECT
                m.movie_id,
                m.title,
                m.length
            FROM collection_contents cc
            JOIN movie m
                ON cc.movie_id = m.movie_id
            JOIN collection c
                ON c.collection_id = cc.collection_id
            WHERE c.collection_id = ? AND c.user_id = ?
            ORDER BY m.title ASC
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, userID);

            try (ResultSet result = statement.executeQuery()) {
                System.out.println();
                System.out.println("  Movies in Collection");
                System.out.println("  --------------------");

                boolean foundAny = false;
                while (result.next()) {
                    foundAny = true;

                    int movieID = result.getInt("movie_id");
                    String title = result.getString("title");
                    int lengthMinutes = result.getInt("length");

                    System.out.printf(
                        "  %d | %s | %s%n",
                        movieID,
                        title,
                        formatMinutes(lengthMinutes)
                    );
                }

                if (!foundAny) {
                    System.out.println("  This collection is empty.");
                }
            }
        }
    }

    // just a check  to make sure the collection actually belongs to the logged-in user
    private void assertCollectionOwned(Connection connect, int userID, int collectionID) throws Exception {
        String sql = """
            SELECT 1
            FROM collection
            WHERE collection_id = ? AND user_id = ?
            """;

        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, userID);

            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new Exception("Collection not found or does not belong to current user.");
                }
            }
        }
    }
// turns total minutes into hours:minutes format like 2:15
    private String formatMinutes(int totalMinutes) {
        int hours = totalMinutes / 60;
        int minutes = totalMinutes % 60;
        return String.format("%d:%02d", hours, minutes);
    }
}