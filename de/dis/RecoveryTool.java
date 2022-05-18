package de.dis;

import de.dis.helper.BufferEntry;
import de.dis.logs.LogEntry;
import de.dis.logs.LogStore;
import de.dis.pages.PageData;
import de.dis.pages.PageStore;

import java.io.IOException;
import java.util.*;

public class RecoveryTool {
    /**
     * Run the recovery tool. The tool will attempt to update all pages for which
     * committed writes newer than the persistent data have been found.
     * <p>
     * If the tool fails to update a stale page, its corresponding BufferEntry
     * will be added to the returned map.
     *
     * @param dry if set to true, the tool will only display the stale pages but not actually recover them yet
     *            (for debugging/analysis purposes)
     *
     * @return a map of Page IDs for stale pages with their corresponding newer BufferEntry
     */
    public static Map<Integer, BufferEntry> run(boolean dry) {
        // analyze log and filter out uncommitted entries
        List<LogEntry> committed = new LinkedList<>(LogStore.getEntries(true));
        // sort by LSN in case log is out of order
        committed.sort(Comparator.comparingInt(LogEntry::lsn));

        // store the latest recovered state and possibly failed recoveries
        Map<Integer, BufferEntry> recoveredStates = new HashMap<>();
        Map<Integer, BufferEntry> failed = new HashMap<>();

        for (LogEntry entry : committed) {
            // set the most recent BufferEntry for the given page
            // replays the write history in sequential order
            recoveredStates.put(entry.pageid(), new BufferEntry(entry.lsn(), entry.taid(), entry.data()));
        }

        for (Map.Entry<Integer, BufferEntry> recoveredState : recoveredStates.entrySet()) {
            int pageid = recoveredState.getKey();

            BufferEntry recovered = recoveredState.getValue();
            PageData persisted = PageStore.read(pageid);

            // if there is no persisted page or the persisted LSN is lower than the
            // latest recovered LSN, the page data is overwritten
            if (persisted == null
                    || (persisted.lsn() < recovered.lsn())) {
                System.out.printf("Found stale page %d\nOld: %s\nNew: %s\n",
                        pageid, persisted, recovered);
                if (!dry) {
                    try {
                        PageStore.write(pageid, new PageData(recovered.lsn(), recovered.data()));
                        System.out.println("Page updated");
                    } catch (IOException e) {
                        System.err.printf("Failed to recover page %d\n%s\n", e.getMessage());
                        failed.put(pageid, recovered);
                    }
                }
            }
        }

        return failed;
    }

    /**
     * Perform a full recovery, **not** as a dry run.
     *
     * @see RecoveryTool#run(boolean)
     */
    public static Map<Integer, BufferEntry> run() {
        return run(false);
    }
}
