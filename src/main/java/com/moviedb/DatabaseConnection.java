package com.moviedb;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Owns the SSH tunnel and JDBC connection to starbug.
 * Everyone calls DatabaseConnection.getConnection() --> nobody sets their own tunnel.
 */
public class DatabaseConnection {

    private static final String SSH_HOST = "starbug.cs.rit.edu";
    private static final int    SSH_PORT = 22;
    private static final int    DB_PORT  = 5432;

    private static Session    sshSession = null;
    private static Connection connection = null;

    // No instantiation, this is a utility class
    private DatabaseConnection() {}

    /**
     * Loads db.properties, opens the SSH tunnel, and connects to Postgres.
     * Safe to call multiple times, returns the existing connection if it's still alive.
     */
    public static Connection getConnection() throws Exception {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }

        Properties props = loadProperties();

        String user     = props.getProperty("db.user");
        String password = props.getProperty("db.password");
        String database = props.getProperty("db.url").split("/")[3]; // extracts 'p32002_XX' from the URL

        // Start up the SSH tunnel first
        sshSession = buildSSHTunnel(user, password);
        int forwardedPort = sshSession.setPortForwardingL(0, "127.0.0.1", DB_PORT);

        // Now connect Postgres through the tunnel
        String url = "jdbc:postgresql://127.0.0.1:" + forwardedPort + "/" + database + "?preferQueryMode=simple";
        Properties dbProps = new Properties();
        dbProps.put("user", user);
        dbProps.put("password", password);

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(url, dbProps);
        connection.createStatement().execute("SET max_parallel_workers_per_gather = 0");
        connection.createStatement().execute("SET max_parallel_workers = 0");
        connection.createStatement().execute("SET max_parallel_maintenance_workers = 0");
        System.out.println("  Connected to database.");
        return connection;
    }

    /**
     * Cleanly closes the JDBC connection and tears down the SSH tunnel.
     * Call this on app exit.
     */
    public static void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("  Warning: couldn't close DB connection cleanly --> " + e.getMessage());
        }

        if (sshSession != null && sshSession.isConnected()) {
            sshSession.disconnect();
        }

        System.out.println("  Disconnected. Goodbye!");
    }

    // --> Private Helpers <--

    private static Properties loadProperties() throws Exception {
        Properties props = new Properties();

        // Maven copies resources/ to the classpath root at build time
        var stream = DatabaseConnection.class.getClassLoader().getResourceAsStream("db.properties");
        if (stream == null) throw new Exception("db.properties not found in resources/. Did you create it?");

        props.load(stream);
        return props;
    }

    private static Session buildSSHTunnel(String user, String password) throws Exception {
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");

        JSch jsch = new JSch();
        Session session = jsch.getSession(user, SSH_HOST, SSH_PORT);
        session.setPassword(password);
        session.setConfig(config);
        session.connect();

        return session;
    }
}