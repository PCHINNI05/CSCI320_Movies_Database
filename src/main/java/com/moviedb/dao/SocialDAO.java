package com.moviedb.dao;
import java.sql.Connection;

public class SocialDAO {
    public void viewFollowers(Connection connect, int userId) {
        // Implementation to view followers of a user
    }
    public void viewFollowing(Connection connect, int userId) {
        // Implementation to view users that a user is following
    }
    public void followUser(Connection connect, int userId, String userEmail) {
        // Implementation to follow a user
    }
    public void unfollowUser(Connection connect, int userId, String userEmail) {
        // Implementation to unfollow a user
    }

}
