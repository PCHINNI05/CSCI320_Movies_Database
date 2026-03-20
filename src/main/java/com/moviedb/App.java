package com.moviedb;

import java.util.List;
import java.util.Scanner;

import com.moviedb.dao.UserDAO;
import com.moviedb.dao.WatchDAO;

import java.sql.Connection;
import com.moviedb.dao.CollectionDAO;
import com.moviedb.dao.MovieDAO;

import com.moviedb.dao.MovieDAO;



/**
 * Entry point. Handles the top-level menu and keeps track of who's logged in.
 * All feature logic lives in the DAOs, this file just drives the UI.
 */
public class App {
    // The logged-in user's ID lives here for the session. -1 means nobody's logged in.
    public static int    currentUserId   = -1;
    public static String currentUsername = null;

    static final Scanner scanner = new Scanner(System.in);

    private static final UserDAO userDAO = new UserDAO();
    private static final MovieDAO movieDAO = new MovieDAO();
    private static final CollectionDAO collectionDAO = new CollectionDAO();
    private static final MovieDAO movieDAO = new MovieDAO();
    private static final WatchDAO watchDAO = new WatchDAO();

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
                case 1  -> handleMovieSearch();
                case 2  -> handleCollections();
                case 3  -> handleWatchMovie();
                case 4  -> handleRateMovie();
                case 5  -> System.out.println("  [Social - coming soon]");
                case 0  -> loggedIn = handleLogout();
                default -> System.out.println("  Not a valid option, try again.");
            }
        }
    }

    // --> Auth stubs (will be replaced when UserDAO is built) <--

    /**
     * Full movie search flow. Pick a filter, enter a term, and browse results.
     * Re-sorting is in-memory so we don't hit the DB twice for same query.
     */
    private static void handleMovieSearch() {
        printDivider();
        System.out.println("  Search movies by:");
        System.out.println("  Search movies by:");
        System.out.println("  1. Title");
        System.out.println("  2. Release Date  (YYYY, YYYY-MM, or YYYY-MM-DD)");
        System.out.println("  3. Cast Member");
        System.out.println("  4. Studio");
        System.out.println("  5. Genre");
        System.out.println("  0. Back");
        printDivider();

        System.out.println("  > ");

        int choice = readInt();
        if (choice == 0) return;

        String term = readLine("Search: ");

        List<MovieDAO.MovieResult> results = switch (choice) {
            case 1  -> movieDAO.searchByTitle(term);
            case 2  -> movieDAO.searchByReleaseDate(term);
            case 3  -> movieDAO.searchByCastMember(term);
            case 4  -> movieDAO.searchByStudio(term);
            case 5  -> movieDAO.searchByGenre(term);
            default -> { System.out.println("  Not a valid option."); yield List.of(); }
        };

        movieDAO.printResults(results);
        if (results.isEmpty()) return;

        // Let them re-sort the same results as many times as they want
        boolean sorting = true;
        while (sorting) {
            printDivider();
            System.out.println("  Re-sort by:   (0 to go back)");
            System.out.println("  1. Title A->Z      2. Title Z->A");
            System.out.println("  3. Studio A->Z     4. Studio Z->A");
            System.out.println("  5. Genre A->Z      6. Genre Z->A");
            System.out.println("  7. Year (oldest)  8. Year (newest)");
            printDivider();
            System.out.print("  > ");

            int sortChoice = readInt();
            if (sortChoice == 0) { sorting = false; continue; }

            results = switch (sortChoice) {
                case 1  -> movieDAO.sort(results, "title",  true);
                case 2  -> movieDAO.sort(results, "title",  false);
                case 3  -> movieDAO.sort(results, "studio", true);
                case 4  -> movieDAO.sort(results, "studio", false);
                case 5  -> movieDAO.sort(results, "genre",  true);
                case 6  -> movieDAO.sort(results, "genre",  false);
                case 7  -> movieDAO.sort(results, "year",   true);
                case 8  -> movieDAO.sort(results, "year",   false);
                default -> { System.out.println("  Not a valid sort option."); yield results; }
            };

            if (sortChoice >= 1 && sortChoice <= 8) movieDAO.printResults(results);
        }
    }

    /**
     * Prompts for credentials and attempts login. On success, stashes the session
     * state and drops the user into the main menu. Error messages come from the DAO.
     */
    private static void handleLogin() {
        System.out.println();
        String username = readLine("Username: ");
        String password = readLine("Password: ");

        int userId = userDAO.login(username, password);
        if (userId == -1) return;

        currentUserId   = userId;
        currentUsername = username;

        System.out.printf("%n  Welcome back, %s!%n", userDAO.getFullName(userId));
        showMainMenu();
    }

    /**
     * Walks the user through account creation, then auto-logs them in so they
     * don't have to immediately turn around and log in again. Duplicate username
     * and email checks happen inside the DAO.
     */
    private static void handleRegister() {
        System.out.println();
        String firstName = readLine("First Name:  ");
        String lastName  = readLine("Last Name:   ");
        String username  = readLine("Username:    ");
        String email     = readLine("Email:       ");
        String password  = readLine("Password:    ");

        int userId = userDAO.register(firstName, lastName, username, email, password);
        if (userId == -1) return;

        currentUserId   = userId;
        currentUsername = username;

        System.out.printf("%n  Account created! Welcome, %s!%n", firstName);
        showMainMenu();
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

    private static void handleCollections() {
        boolean inCollectionsMenu = true;

        while (inCollectionsMenu) {
            printDivider();
            System.out.println("  My Collections");
            printDivider();
            System.out.println("  1. View My Collections");
            System.out.println("  2. Create Collection");
            System.out.println("  3. Rename Collection");
            System.out.println("  4. Delete Collection");
            System.out.println("  5. View Collection Details");
            System.out.println("  6. Add Movie to Collection");
            System.out.println("  7. Remove Movie from Collection");
            System.out.println("  0. Back");
            printDivider();
            System.out.print("  > ");

            switch (readInt()) {
                case 1  -> viewMyCollections();
                case 2  -> createCollection();
                case 3  -> renameCollection();
                case 4  -> deleteCollection();
                case 5  -> viewCollectionDetails();
                case 6  -> addMovieToCollection();
                case 7  -> removeMovieFromCollection();
                case 0  -> inCollectionsMenu = false;
                default -> System.out.println("  Not a valid option, try again.");
            }
        }
    }

    /**
     * Fetches and displays all collections belonging to the logged-in user, along with their movie counts and total runtimes.  
     */
    private static void viewMyCollections() {
        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.getUserCollections(conn, currentUserId);
        } catch (Exception e) {
            System.out.println("  Error loading collections: " + e.getMessage());
        }
    }

    /**
     * Prompts for a collection name and creates a new collection owned by the logged-in user.
     */
    private static void createCollection() {
        String collectionName = readLine("Collection Name: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.createCollection(conn, currentUserId, collectionName);
            System.out.println("  Collection created.");
        } catch (Exception e) {
            System.out.println("  Error creating collection: " + e.getMessage());
        }
    }

    /**
     * Prompts for a collection ID and new name, then renames the collection if it belongs to the logged-in user.
     */
    private static void renameCollection() {
        int collectionId = readIntPrompt("Collection ID to rename: ");
        String newName = readLine("New Name: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.renameCollection(conn, currentUserId, collectionId, newName);
            System.out.println("  Collection renamed.");
        } catch (Exception e) {
            System.out.println("  Error renaming collection: " + e.getMessage());
        }
    }

    /**
     * Prompts for a collection ID and deletes the collection if it belongs to the logged-in user. Also deletes all movies inside the collection via cascading.
     */
    private static void deleteCollection() {
        int collectionId = readIntPrompt("Collection ID to delete: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.deleteCollection(conn, currentUserId, collectionId);
            System.out.println("  Collection deleted.");
        } catch (Exception e) {
            System.out.println("  Error deleting collection: " + e.getMessage());
        }
    }

    /**
     * Prompts for a collection ID and displays all movies inside that collection, along with their details. Only works if the collection belongs to the logged-in user.
     */
    private static void viewCollectionDetails() {
        int collectionId = readIntPrompt("Collection ID: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.getCollectionDetails(conn, currentUserId, collectionId);
        } catch (Exception e) {
            System.out.println("  Error loading collection details: " + e.getMessage());
        }
    }

    /**
     *  Prompts for a collection ID and movie ID, then adds that movie to the collection if it belongs to the logged-in user. Movie ID must be valid, but there's no check to prevent duplicates (a movie can be added multiple times).
     */
    private static void addMovieToCollection() {
        int collectionId = readIntPrompt("Collection ID: ");
        int movieId = readIntPrompt("Movie ID to add: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.addMovieToCollection(conn, currentUserId, collectionId, movieId);
            System.out.println("  Movie added to collection.");
        } catch (Exception e) {
            System.out.println("  Error adding movie: " + e.getMessage());
        }
    }

    /**
     * Prompts for a collection ID and movie ID, then removes that movie from the collection if it belongs to the logged-in user. If the movie appears multiple times, only one instance will be removed.
     */
    private static void removeMovieFromCollection() {
        int collectionId = readIntPrompt("Collection ID: ");
        int movieId = readIntPrompt("Movie ID to remove: ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            collectionDAO.removeMovieFromCollection(conn, currentUserId, collectionId, movieId);
            System.out.println("  Movie removed from collection.");
        } catch (Exception e) {
            System.out.println("  Error removing movie: " + e.getMessage());
        }
    }

    private static void handleRateMovie() {
        int movieId = readIntPrompt("Movie ID to rate: ");
        int rating = readIntPrompt("Star rating (1-5): ");

        try {
            Connection conn = DatabaseConnection.getConnection();
            movieDAO.rateMovie(conn, currentUserId, movieId, rating);
            System.out.println("  Movie rated.");
        } catch (Exception e) {
            System.out.println("  Error rating movie: " + e.getMessage());
        }
    }

    private static void handleWatchMovie() {
        printDivider();
        System.out.println("  1. Watch a single movie");
        System.out.println("  2. Play an entire collection");
        System.out.println("  0. Back");
        printDivider();
        System.out.print("  > ");

        switch (readInt()) {
            case 1 -> {
                int movieId = readIntPrompt("Movie ID to watch: ");
                try {
                    watchDAO.watchMovie(DatabaseConnection.getConnection(), currentUserId, movieId);
                    System.out.println("  Marked as watched!");
                } catch (Exception e) {
                    System.out.println("  Error recording watch: " + e.getMessage());
                }
            }
            case 2 -> {
                int collectionId = readIntPrompt("Collection ID to play: ");
                try {
                    watchDAO.watchCollection(DatabaseConnection.getConnection(), currentUserId, collectionId);
                    System.out.println("  Entire collection marked as watched!");
                } catch (Exception e) {
                    System.out.println("  Error recording collection watch: " + e.getMessage());
                }
            }
            case 0 -> {}
            default -> System.out.println("  Not a valid option.");
        }
    }

    private static int readIntPrompt(String prompt) {
        while (true) {
            System.out.print("  " + prompt);
            try {
                String line = scanner.nextLine().trim();
                return Integer.parseInt(line);
            } catch (NumberFormatException e) {
                System.out.println("  Please enter a valid number.");
            }
        }
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