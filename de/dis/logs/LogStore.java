package de.dis.logs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class LogStore {
    private static final String LOG_FILE_PATH = "./store/logfile.txt";

    private static int _lsn = getNextLSN();

    public static void clear() {
        File logfile = new File(LOG_FILE_PATH);
        logfile.delete();
    }

    /**
     * Log a write operation.
     *
     * @param taid   ID of the modifying transaction
     * @param pageid ID of the page being modified
     * @param data   Data to be written to the given page
     * @return the LSN of the log entry written
     * @throws IOException if an error occurs accessing/writing to the logfile
     */
    public static int addEntry(int taid, int pageid, String data) throws IOException {
        return addEntry(taid, String.format("%d,%s", pageid, data));
    }

    /**
     * Log the start or end of a transaction.
     *
     * @param taid   ID of the affected transaction
     * @param action the action that is being logged
     * @return the LSN of the log entry written
     * @throws IOException if an error occurs accessing/writing to the logfile
     */
    public static int addEntry(int taid, LogAction action) throws IOException {
        return addEntry(taid, action.code());
    }

    private static int addEntry(int taid, String data) throws IOException {
        int lsn = _lsn++;
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true)) {
            writer.write(String.format("%d,%d,%s\n", lsn, taid, data));
        }
        return lsn;
    }

    /**
     * Get the next available LSN from the current logfile.
     *
     * @return the next unused LSN
     */
    private static int getNextLSN() {
        File file = new File(LOG_FILE_PATH);
        if (!file.isFile()) return 0;
        try {
            Scanner s = new Scanner(file);
            String[] lines = s.tokens().toArray(String[]::new);
            if (lines.length == 0) return 0;
            String last = lines[lines.length - 1];
            String[] entries = last.split(",");
            if (entries.length < 3) return 0;
            try {
                int lsn = Integer.parseInt(entries[0]);
                return lsn + 1;
            } catch (NumberFormatException e) {
                System.err.printf("Malformed last log entry %s\n", last);
                return 0;
            }
        } catch (FileNotFoundException e) {
            return 0;
        }
    }

    /**
     * Get all entries currently in the log file.
     *
     * @param skipUncommitted whether to skip entries from transactions that have not yet been committed
     * @return a list of log entries
     */
    public static List<LogEntry> getEntries(boolean skipUncommitted) {
        List<LogEntry> logEntries = new ArrayList<>();
        Set<Integer> uncommitted = new HashSet<>();

        File file = new File(LOG_FILE_PATH);
        if (file.isFile()) {
            try {
                Scanner s = new Scanner(file);
                while (s.hasNextLine()) {
                    String line = s.nextLine();
                    try {
                        // parse one log entry line
                        String[] entries = line.split(",");
                        int lsn = Integer.parseInt(entries[0]);
                        int taid = Integer.parseInt(entries[1]);
                        if (entries.length == 3 && skipUncommitted) {
                            // if the line is a BOT or EOT marker and we are tracking commits,
                            // use the action marker to track the commit state of this transaction
                            String action = entries[2];
                            if (LogAction.TRANSACTION_END.code().equals(action)) {
                                uncommitted.remove(taid);
                            } else if (LogAction.TRANSACTION_BEGIN.code().equals(action)) {
                                uncommitted.add(taid);
                            }
                        } else if (entries.length == 4) {
                            int pageid = Integer.parseInt(entries[2]);
                            String data = entries[3];
                            logEntries.add(new LogEntry(lsn, taid, pageid, data));
                        }
                    } catch (NumberFormatException e) {
                        System.err.printf("Malformed log entry %s\n", line);
                    }
                }
            } catch (FileNotFoundException e) {
                System.err.println("An error occurred while trying to read from the persistent storage.");
                e.printStackTrace();
            }
        }

        return !skipUncommitted
                ? logEntries
                : logEntries.stream().filter(entry -> !uncommitted.contains(entry.taid())).toList();
    }

    /**
     * Get all entries currently in the log file.
     *
     * @return a list of all log entries, including uncommitted writes
     */
    public static List<LogEntry> getEntries() {
        return getEntries(false);
    }
}
