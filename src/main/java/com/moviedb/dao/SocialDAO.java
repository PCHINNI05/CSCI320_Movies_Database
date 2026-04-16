/**
 * FILE: SocialDAO.java
 *
 * DESCRIPTION:
 *   Data access object for social graph operations.
 *   Handles displaying follower and following lists, following new users by email,
 *   and unfollowing existing ones.
 *
 * AUTHORS:
 *   - Ibtehaz Rafid     (ir9269)
 *   - Samuel Stewart    (ses1251)
 *   - Praneel Chinni    (pjc8054)
 * 
 * COURSE:  CSCI 320 - Principles of Data Management
 * SECTION: 02
 * TERM:    Spring 2026
 * GROUP:   #18
 */
package com.moviedb.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Data access object for social graph operations.
 * <p>
 * Handles displaying follower and following lists, following new users by email,
 * and unfollowing existing ones.
 */
public class SocialDAO {

    /**
     * Prints all users who follow the given user to standard output,
     * showing each follower's username and full name.
     *
     * @param connect active database connection
     * @param userId  the ID of the user whose follower list should be displayed
     */
    public void viewFollowers(Connection connect, int userId) {
        String sql = """
                SELECT u.user_id, u.username, u.first_name, u.last_name
                FROM follows f
                JOIN users u ON f.follower_id = u.user_id
                WHERE f.followee_id = ?
                ORDER BY u.username
                """;

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, userId);
            ResultSet rs = stmt.executeQuery();

            boolean foundAny = false;
            while (rs.next()) {
                foundAny = true;
                System.out.println("  @" + rs.getString("username")
                        + " - "
                        + rs.getString("first_name") + " "
                        + rs.getString("last_name"));
            }

            if (!foundAny) {
                System.out.println("  You don't have any followers yet.");
            }

        } catch (Exception e) {
            System.err.println("  Couldn't load followers: " + e.getMessage());
        }
    }

    /**
     * Prints all users the given user is following to standard output,
     * showing each followee's username and full name.
     *
     * @param connect active database connection
     * @param userId  the ID of the user whose following list should be displayed
     */
    public void viewFollowing(Connection connect, int userId) {
        String sql = """
                SELECT u.user_id, u.username, u.first_name, u.last_name
                FROM follows f
                JOIN users u ON f.followee_id = u.user_id
                WHERE f.follower_id = ?
                ORDER BY u.username
                """;

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, userId);
            ResultSet rs = stmt.executeQuery();

            boolean foundAny = false;
            while (rs.next()) {
                foundAny = true;
                System.out.println("  @" + rs.getString("username")
                        + " - "
                        + rs.getString("first_name") + " "
                        + rs.getString("last_name"));
            }

            if (!foundAny) {
                System.out.println("  You aren't following anyone yet.");
            }

        } catch (Exception e) {
            System.err.println("  Couldn't load following list: " + e.getMessage());
        }
    }

    /**
     * Creates a follow relationship from the given user to the user identified by email.
     * <p>
     * No action is taken if the target email does not exist, if the user attempts
     * to follow themselves, or if the relationship already exists.
     *
     * @param connect   active database connection
     * @param userId    the ID of the user initiating the follow
     * @param userEmail the email address of the user to follow
     */
    public void followUser(Connection connect, int userId, String userEmail) {
        int targetUserId = getUserIdByEmail(connect, userEmail);

        if (targetUserId == -1) {
            System.out.println("  No account found with that email.");
            return;
        }

        if (targetUserId == userId) {
            System.out.println("  You can't follow yourself.");
            return;
        }

        if (isAlreadyFollowing(connect, userId, targetUserId)) {
            System.out.println("  You are already following that user.");
            return;
        }

        String sql = """
                INSERT INTO follows (follower_id, followee_id)
                VALUES (?, ?)
                """;

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, userId);
            stmt.setInt(2, targetUserId);
            stmt.executeUpdate();
            System.out.println("  Successfully followed that user.");
        } catch (Exception e) {
            System.err.println("  Couldn't follow user: " + e.getMessage());
        }
    }

    /**
     * Removes a follow relationship from the given user to the user identified by email.
     * <p>
     * No action is taken if the target email does not exist or if no follow
     * relationship currently exists.
     *
     * @param connect   active database connection
     * @param userId    the ID of the user initiating the unfollow
     * @param userEmail the email address of the user to unfollow
     */
    public void unfollowUser(Connection connect, int userId, String userEmail) {
        int targetUserId = getUserIdByEmail(connect, userEmail);

        if (targetUserId == -1) {
            System.out.println("  No account found with that email.");
            return;
        }

        if (!isAlreadyFollowing(connect, userId, targetUserId)) {
            System.out.println("  You are not following that user.");
            return;
        }

        String sql = """
                DELETE FROM follows
                WHERE follower_id = ? AND followee_id = ?
                """;

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, userId);
            stmt.setInt(2, targetUserId);
            stmt.executeUpdate();
            System.out.println("  Successfully unfollowed that user.");
        } catch (Exception e) {
            System.err.println("  Couldn't unfollow user: " + e.getMessage());
        }
    }

    // --- private helpers ---

    private int getUserIdByEmail(Connection connect, String email) {
        String sql = "SELECT user_id FROM users WHERE email = ?";

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setString(1, email);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("user_id");
            }
        } catch (Exception e) {
            System.err.println("  Couldn't look up user by email: " + e.getMessage());
        }

        return -1;
    }

    private boolean isAlreadyFollowing(Connection connect, int followerId, int followeeId) {
        String sql = """
                SELECT 1
                FROM follows
                WHERE follower_id = ? AND followee_id = ?
                """;

        try (PreparedStatement stmt = connect.prepareStatement(sql)) {
            stmt.setInt(1, followerId);
            stmt.setInt(2, followeeId);
            return stmt.executeQuery().next();
        } catch (Exception e) {
            return false;
        }
    }
}