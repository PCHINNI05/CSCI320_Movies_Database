package com.moviedb.dao;

import com.moviedb.DatabaseConnection;

import java.sql.*;

import org.mindrot.jbcrypt.BCrypt;

/**
 * Everything user auth --> registration, login, and keeping last_access_date updated.
 */
public class UserDAO {

    /**
     * Registers a new user and returns their generated user_id.
     * Returns -1 if the username or email is already taken.
     */
    public int register(String firstName, String lastName,
                        String username, String email, String password) {
        // Check for duplicate username or email first. Cleaner error messages this way
        if (existsByUsername(username)) {
            System.out.println("  That username is already taken.");
            return -1;
        }
        if (existsByEmail(email)) {
            System.out.println("  An account with that email already exists.");
            return -1;
        }

        String sql = """
                INSERT INTO users (first_name, last_name, username, email, password,
                                   creation_date, last_access_date)
                VALUES (?, ?, ?, ?, ?, NOW(), NOW())
                RETURNING user_id
                """;

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setString(1, firstName);
            stmt.setString(2, lastName);
            stmt.setString(3, username);
            stmt.setString(4, email);
            String hashed = BCrypt.hashpw(password, BCrypt.gensalt());
            stmt.setString(5, hashed);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) return rs.getInt("user_id");

        } catch (Exception e) {
            System.err.println("  Registration failed: " + e.getMessage());
        }

        return -1;
    }

    /**
     * Validates credentials and returns the user_id on success, -1 on failure.
     * Also bumps last_access_date on every successful login.
     */
    public int login(String username, String password) {
        String sql = """
                SELECT user_id, password
                FROM users
                WHERE username = ?
                """;

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setString(1, username);
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                System.out.println("  No account found with that username.");
                return -1;
            }

            // Hashing done now for consistency
            if (!BCrypt.checkpw(password, rs.getString("password"))) {
                System.out.println("  Wrong password.");
                return -1;
            }

            int userId = rs.getInt("user_id");
            updateLastAccess(userId);
            return userId;

        } catch (Exception e) {
            System.err.println("  Login failed: " + e.getMessage());
        }

        return -1;
    }

    /**
     * Stamps the current timestamp onto last_access_date for the given user.
     * Called on every successful login.
     */
    private void updateLastAccess(int userId) {
        String sql = "UPDATE users SET last_access_date = NOW() WHERE user_id = ?";

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setInt(1, userId);
            stmt.executeUpdate();
        } catch (Exception e) {
            // Non-fatal, login still succeeded, just log it
            System.err.println("  Warning: couldn't update last access time - " + e.getMessage());
        }
    }

    /** Fetches the display name (first + last) for a given user_id. */
    public String getFullName(int userId) {
        String sql = "SELECT first_name, last_name FROM users WHERE user_id = ?";

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setInt(1, userId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("first_name") + " " + rs.getString("last_name");
            }
        } catch (Exception e) {
            System.err.println("  Couldn't fetch user name: " + e.getMessage());
        }

        return "Unknown";
    }

    // --- private helpers ---

    private boolean existsByUsername(String username) {
        return existsBy("username", username);
    }

    private boolean existsByEmail(String email) {
        return existsBy("email", email);
    }

    private boolean existsBy(String column, String value) {
        String sql = "SELECT 1 FROM users WHERE " + column + " = ?";

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setString(1, value);
            return stmt.executeQuery().next();
        } catch (Exception e) {
            return false;
        }
    }
}