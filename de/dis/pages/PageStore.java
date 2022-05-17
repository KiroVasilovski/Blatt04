package de.dis.pages;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

class PageFormatException extends Exception {
    public PageFormatException(String message) {
        super(message);
    }
}

public class PageStore {
    public static final int MIN_PAGE = 10;
    public static final int MAX_PAGE = 59;

    private static final String PAGE_PATH = "./store/";

    private static String getPath(int id) {
        return String.format("%s%d.txt", PAGE_PATH, id);
    }

    /**
     * Read the current persisted data from a single page.
     *
     * @param id the page to read from
     * @return the data on the page, or null if it does not exist
     */
    public static PageData read(int id) {
        if (id < MIN_PAGE || id > MAX_PAGE)
            throw new IllegalArgumentException(String.format("Page ID must be between %d and %d", MIN_PAGE, MAX_PAGE));

        File file = new File(getPath(id));
        if (file.isFile()) {
            try {
                Scanner s = new Scanner(file);
                if (s.hasNextLine()) {
                    String line = s.nextLine();
                    try {
                        return readLine(line);
                    } catch (PageFormatException e) {
                        System.err.printf("Malformed page %d\nPage data: %s\n%s\n", id, line, e.getMessage());
                    }
                }
            } catch (FileNotFoundException e) {
                return null;
            }
        }
        return null;
    }

    private static PageData readLine(String line) throws PageFormatException {
        String[] entries = line.split(",");
        if (entries.length != 2) throw new PageFormatException("Invalid number of comma-separated values");

        try {
            int lsn = Integer.parseInt(entries[0]);
            String data = entries[1];
            return new PageData(lsn, data);
        } catch (NumberFormatException e) {
            throw new PageFormatException("Could not parse LSN as integer");
        }
    }

    /**
     * Write the given page data to permanent DB.
     *
     * @param id    the Page ID to write to
     * @param page  the data to write to the page
     * @throws IOException
     */
    public static void write(int id, PageData page) throws IOException {
        if (page == null) return;

        try (FileWriter writer = new FileWriter(getPath(id), false)) {
            writer.write(String.format("%d,%s", page.lsn(), page.data()));
        }
    }
}
