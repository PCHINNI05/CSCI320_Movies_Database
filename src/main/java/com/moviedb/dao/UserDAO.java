/**
 * FILE: UserDAO.java
 *
 * DESCRIPTION:
 *   Data access object for user authentication and profile operations.
 *   Handles account registration, login with BCrypt password verification,
 *   last-access timestamp updates, and user profile display.
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

import com.moviedb.DatabaseConnection;

import java.sql.*;

import org.mindrot.jbcrypt.BCrypt;

/**
 * Data access object for user authentication and profile operations.
 * <p>
 * Handles account registration, login, last-access tracking, and profile display.
 * Passwords are hashed with BCrypt before storage and verified with
 * {@link org.mindrot.jbcrypt.BCrypt#checkpw} on login.
 */
public class UserDAO {

    /**
     * Registers a new user account and returns the generated {@code user_id}.
     * <p>
     * Checks for a duplicate username and email before inserting. The password is
     * hashed with BCrypt (cost factor 10) prior to storage.
     *
     * @param firstName the user's first name
     * @param lastName  the user's last name
     * @param username  the desired username (must be unique)
     * @param email     the user's email address (must be unique)
     * @param password  the plaintext password to hash and store
     * @return the newly created {@code user_id}, or {@code -1} if the username or
     *         email is already taken, or if the insert fails
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
     * Validates login credentials and returns the matching {@code user_id}.
     * <p>
     * On success, {@code last_access_date} is updated to the current timestamp.
     *
     * @param username the account username to look up
     * @param password the plaintext password to verify against the stored BCrypt hash
     * @return the authenticated {@code user_id}, or {@code -1} if the username is
     *         not found or the password does not match
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

    /**
     * Returns the full display name (first name + last name) for the given user.
     *
     * @param userId the {@code user_id} to look up
     * @return the user's full name as a single string, or {@code "Unknown"} if not found
     */
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

    /**
     * Looks up a {@code user_id} by email address.
     *
     * @param email the email address to search for
     * @return the matching {@code user_id}, or {@code -1} if no account is found
     */
    public int getUserIdByEmail(String email) {
        String sql = "SELECT user_id FROM users WHERE email = ?";

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setString(1, email);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) return rs.getInt("user_id");
        } catch (Exception e) {
            System.err.println("  Couldn't look up user by email: " + e.getMessage());
        }

        return -1;
    }

    /**
     * Looks up a {@code user_id} by username.
     *
     * @param username the username to search for
     * @return the matching {@code user_id}, or {@code -1} if no account is found
     */
    public int getUserIdByUsername(String username) {
        String sql = "SELECT user_id FROM users WHERE username = ?";

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setString(1, username);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) return rs.getInt("user_id");
        } catch (Exception e) {
            System.err.println("  Couldn't look up user by username: " + e.getMessage());
        }
        
        return -1;
    }

    /**
     * Prints a profile summary for the given user to standard output.
     * <p>
     * Displays the user's full name, username, collection count, follower count,
     * and following count. Top 10 movies are fetched and printed separately via
     * {@link MovieDAO}.
     *
     * @param userId the {@code user_id} whose profile should be displayed
     */
    public void showProfile(int userId) {
        String sql = """
                SELECT
                    u.username,
                    u.first_name,
                    u.last_name,
                    (SELECT COUNT(*) FROM collection  WHERE user_id    = u.user_id) AS collection_count,
                    (SELECT COUNT(*) FROM follows      WHERE followee_id = u.user_id) AS follower_count,
                    (SELECT COUNT(*) FROM follows      WHERE follower_id = u.user_id) AS following_count
                FROM users u
                WHERE u.user_id = ?
                """;

        try (PreparedStatement stmt = DatabaseConnection.getConnection().prepareStatement(sql)) {
            stmt.setInt(1, userId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                System.out.println();
                System.out.printf("  %s %s (@%s)%n",
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("username"));
                System.out.println("  ----------------------------------------");
                System.out.printf("  Collections:  %d%n", rs.getInt("collection_count"));
                System.out.printf("  Followers:    %d%n", rs.getInt("follower_count"));
                System.out.printf("  Following:    %d%n", rs.getInt("following_count"));
            } else {
                System.out.println("  User not found.");
            }
        } catch (Exception e) {
            System.err.println("  Couldn't load profile: " + e.getMessage());
        }
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