/**
 * FILE: CollectionDAO.java
 *
 * DESCRIPTION:
 *   Data access object for movie collection management.
 *   Provides create, rename, delete, and content-management operations for
 *   user-owned collections. All mutating methods verify collection ownership
 *   before executing to prevent cross-user modifications.
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
import java.sql.ResultSet;

/**
 * Data access object for movie collection management.
 * <p>
 * Provides create, rename, delete, and content-management operations for
 * user-owned collections. All mutating methods verify collection ownership
 * before executing to prevent cross-user modifications.
 */
public class CollectionDAO {

    /**
     * Creates a new, empty collection owned by the specified user.
     *
     * @param connect        active database connection
     * @param userID         the ID of the user creating the collection
     * @param collectionName the display name for the new collection
     * @throws Exception if the insert fails
     */
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

    /**
     * Renames an existing collection owned by the specified user.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user who owns the collection
     * @param collectionID the ID of the collection to rename
     * @param newName      the new display name for the collection
     * @throws Exception if the collection does not belong to the user, or the update fails
     */
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

    /**
     * Deletes a collection and all of its movie associations.
     * <p>
     * Removes all rows from {@code collection_contents} for this collection first
     * to satisfy foreign key constraints before deleting the collection itself.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user who owns the collection
     * @param collectionID the ID of the collection to delete
     * @throws Exception if the collection does not belong to the user or a delete fails
     */
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
    /**
     * Adds a movie to one of the user's collections.
     * <p>
     * If the movie is already present in the collection the insert is silently
     * ignored via {@code ON CONFLICT DO NOTHING} and a message is printed.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user who owns the collection
     * @param collectionID the ID of the target collection
     * @param movieID      the ID of the movie to add
     * @throws Exception if the collection does not belong to the user or the insert fails
     */
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

    /**
     * Removes a movie from one of the user's collections.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user who owns the collection
     * @param collectionID the ID of the collection to modify
     * @param movieID      the ID of the movie to remove
     * @throws Exception if the collection does not belong to the user, the movie is not
     *                   in the collection, or the delete fails
     */
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

    /**
     * Prints a summary of all collections owned by the given user to standard output.
     * <p>
     * Each row shows the collection ID, name, movie count, and total runtime
     * formatted as {@code H:MM}.
     *
     * @param connect active database connection
     * @param userID  the ID of the user whose collections should be listed
     * @throws Exception if the query fails
     */
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

    /**
     * Prints the movie contents of a specific collection to standard output.
     * <p>
     * Lists each movie's ID, title, and formatted runtime in ascending title order.
     * Only accessible if the collection belongs to the specified user.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user who owns the collection
     * @param collectionID the ID of the collection to inspect
     * @throws Exception if the collection does not belong to the user or the query fails
     */
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

    /**
     * Verifies that the specified collection belongs to the specified user.
     *
     * @param connect      active database connection
     * @param userID       the ID of the user to check ownership for
     * @param collectionID the ID of the collection to verify
     * @throws Exception if no matching collection/user pair is found
     */
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

    /**
     * Converts a total minute count into an {@code H:MM} formatted string.
     *
     * @param totalMinutes the total duration in minutes
     * @return a string in {@code H:MM} format (e.g. {@code "2:05"} for 125 minutes)
     */
    private String formatMinutes(int totalMinutes) {
        int hours = totalMinutes / 60;
        int minutes = totalMinutes % 60;
        return String.format("%d:%02d", hours, minutes);
    }
}