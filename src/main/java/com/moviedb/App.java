package com.moviedb;

import java.util.Scanner;

/**
 * Entry point. Handles the top-level menu and keeps track of who's logged in.
 * All feature logic lives in the DAOs, this file just drives the UI.
 */
public class App {
    // The logged-in user's ID lives here for the session. -1 means nobody's logged in.
    public static int    currentUserId   = -1;
    public static String currentUsername = null;

    static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        printBanner();

        // Connect once on startup --> DAOs reuse this connection throughout
        try {
            DatabaseConnection.getConnection();
        } catch (Exception e) {
            System.err.println("  Couldn't connect to the database: " + e.getMessage());
            System.err.println("  Check your db.properties and make sure you're on RIT's network (or VPN).");
            return;
        }

        // Landing menu --> shown before a user logs in
        boolean running = true;
        while (running) {
            running = showLandingMenu();
        }

        DatabaseConnection.close();
    }

    // --> Menus <--

    /**
     * The pre-login menu. Returns false when the user decides to exit.
     */
    private static boolean showLandingMenu() {
        printDivider();
        System.out.println("  1. Login");
        System.out.println("  2. Create Account");
        System.out.println("  0. Exit");
        printDivider();
        System.out.print("  > ");

        switch (readInt()) {
            case 1  -> handleLogin();
            case 2  -> handleRegister();
            case 0  -> { return false; }
            default -> System.out.println("  Not a valid option, try again.");
        }

        return true;
    }

    /**
     * The main app menu --> shown after a user is logged in.
     * Loops until the user logs out.
     */
    private static void showMainMenu() {
        boolean loggedIn = true;
        while (loggedIn) {
            printDivider();
            System.out.printf("  Logged in as: %s%n", currentUsername);
            printDivider();
            System.out.println("  1. Search Movies");
            System.out.println("  2. My Collections");
            System.out.println("  3. Watch a Movie");
            System.out.println("  4. Rate a Movie");
            System.out.println("  5. Social (Follow / Unfollow)");
            System.out.println("  0. Logout");
            printDivider();
            System.out.print("  > ");

            switch (readInt()) {
                case 1  -> System.out.println("  [Movie Search - coming soon]");
                case 2  -> System.out.println("  [Collections - coming soon]");
                case 3  -> System.out.println("  [Watch - coming soon]");
                case 4  -> System.out.println("  [Ratings - coming soon]");
                case 5  -> System.out.println("  [Social - coming soon]");
                case 0  -> loggedIn = handleLogout();
                default -> System.out.println("  Not a valid option, try again.");
            }
        }
    }

    // --> Auth stubs (will be replaced when UserDAO is built) <--

    private static void handleLogin() {
        System.out.println("  [Login - coming soon]");
        // TODO: prompt credentials --> UserDAO.login() --> set currentUserId/currentUsername --> showMainMenu()
    }

    private static void handleRegister() {
        System.out.println("  [Register - coming soon]");
        // TODO: prompt details --> UserDAO.register() --> auto-login --> showMainMenu()
    }

    /**
     * Clears the session and goes back to the "landing menu".
     * Returns false to break the main menu loop.
     */
    private static boolean handleLogout() {
        System.out.printf("  See you later, %s!%n", currentUsername);
        currentUserId   = -1;
        currentUsername = null;
        return false;
    }

    // --> UI Helpers <--

    private static void printBanner() {
        System.out.println();
        System.out.println("  ╔══════════════════════════════════╗");
        System.out.println("  ║     CSCI320 Movie Streaming      ║");
        System.out.println("  ║         CSCI 320 - RIT           ║");
        System.out.println("  ╚══════════════════════════════════╝");
        System.out.println();
        System.out.println("  Connecting to database...");
    }

    private static void printDivider() {
        System.out.println();
        System.out.println("  ──────────────────────────────────");
    }

    /**
     * Reads an int from stdin. Returns -1 if the input isn't a number
     * so the menu's default case handles it correctly.
     */
    public static int readInt() {
        try {
            String line = scanner.nextLine().trim();
            System.out.println(); // breathe a little
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            System.out.println();
            return -1;
        }
    }

    /** Reads a non-empty string, re-prompting if the user just hits Enter. */
    public static String readLine(String prompt) {
        String input;
        do {
            System.out.print("  " + prompt);
            input = scanner.nextLine().trim();
        } while (input.isEmpty());
        return input;
    }

    /** Reads a string that's allowed to be empty (optional fields). */
    public static String readOptionalLine(String prompt) {
        System.out.println("  " + prompt);
        return scanner.nextLine().trim();
    }
}