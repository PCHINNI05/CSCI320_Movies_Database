package com.moviedb.dao;

import java.sql.Connection;

public class CollectionDAO {
    public void createCollection(Connection connect, int userID, String collectionName) throws Exception {
        String sql = "INSERT INTO collection (collection_name, user_id) VALUES (?, ?)";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setString(1, collectionName);
            statement.setInt(2, userID);
            statement.executeUpdate();
        }
    }

    public void renameCollection(Connection connect, int collectionID, String newName) throws Exception {
        String sql = "UPDATE collection SET collection_name = ? WHERE collection_id = ?";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setString(1, newName);
            statement.setInt(2, collectionID);
            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Collection not found");
            }
        }
    }

    public void deleteCollection(Connection connect, int collectionID) throws Exception {
        String sql = "DELETE FROM collection WHERE collection_id = ?";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Collection not found");
            }
        }
    }



    public void addMovieToCollection(Connection connect, int collectionID, int movieID) throws Exception {
        String sql = "INSERT INTO collection_contents (collection_id, movie_id) VALUES (?, ?)";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, movieID);
            statement.executeUpdate();
        }
    }

    public void removeMovieFromCollection(Connection connect, int collectionID, int movieID) throws Exception {
        String sql = "DELETE FROM collection_contents WHERE collection_id = ? AND movie_id = ?";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            statement.setInt(2, movieID);
            int rows = statement.executeUpdate();
            if (rows == 0) {
                throw new Exception("Movie not found in collection or collection not found");
            }
        }
    }


    public void getUserCollections(Connection connect, int userID) throws Exception {
        String sql = "SELECT * FROM collection WHERE user_id = ?";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, userID);
            var result = statement.executeQuery();
            while (result.next()) {
                int collectID = result.getInt("collection_id");
                String collectionName = result.getString("collection_name");
                String creationDate = result.getTimestamp("creation_date").toString();
                System.out.println(collectID + " | " + collectionName + " | " + creationDate);
            }
        }
    }

    public void getCollectionDetails(Connection connect, int collectionID) throws Exception {
        String sql = "SELECT * FROM collection_contents WHERE collection_id = ?";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setInt(1, collectionID);
            var result = statement.executeQuery();
            while (result.next()) {
                int movieID = result.getInt("movie_id");
                System.out.println(movieID);
            }
        }
    }

}
