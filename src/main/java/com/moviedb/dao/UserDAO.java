package com.moviedb.dao;
import java.sql.Connection;

public class UserDAO {
    public void createUser(Connection connect, String fName, String lName, String username, String email, String password) throws Exception {
        String sql = "INSERT INTO users (first_name, last_name, username, email, password, last_access_date) VALUES (?, ?, ?, ?, ?, ?)";
        try (var statement = connect.prepareStatement(sql)) {
            statement.setString(1, fName);
            statement.setString(2, lName);
            statement.setString(3, username);
            statement.setString(4, email);
            statement.setString(5, password); //NEED TO HASH THIS
            statement.executeUpdate();
        }
    }

    
}
