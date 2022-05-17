package de.dis;

import de.dis.helper.BufferEntry;
import de.dis.logs.LogEntry;
import de.dis.logs.LogStore;
import de.dis.pages.PageData;
import de.dis.pages.PageStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RecoveryTool {
    /**
     * Run the recovery tool. The tool will attempt to update all pages for which
     * committed writes newer than the persistent data have been found.
     * <p>
     * If the tool fails to update a stale page, its corresponding BufferEntry
     * will be added to the returned map.
     *
     * @return a map of Page IDs for stale pages with their corresponding newer BufferEntry
     */
    public static Map<Integer, BufferEntry> run() {
        List<LogEntry> winners = LogStore.getEntries(true);
        Map<Integer, BufferEntry> recoveredStates = new HashMap<>();
        Map<Integer, BufferEntry> failed = new HashMap<>();

        for (LogEntry entry : winners) {
            recoveredStates.put(entry.pageid(), new BufferEntry(entry.lsn(), entry.taid(), entry.data()));
        }

        for (Map.Entry<Integer, BufferEntry> recoveredState : recoveredStates.entrySet()) {
            int pageid = recoveredState.getKey();

            BufferEntry recovered = recoveredState.getValue();
            PageData persisted = PageStore.read(pageid);

            if (persisted == null ||
                    (persisted.lsn() < recovered.lsn()
                            && !persisted.data().equals(recovered.data()))) {
                System.out.printf("Recovering page %d\nOld: %d,%s\nNew: %d,%s\n",
                        pageid, persisted.lsn(), persisted.data(), recovered.lsn(), recovered.data());
                try {
                    PageStore.write(pageid, new PageData(recovered.lsn(), recovered.data()));
                } catch (IOException e) {
                    System.err.printf("Failed to recover page %d\n%s\n", e.getMessage());
                    failed.put(pageid, recovered);
                }
            }
        }

        return failed;
    }
}
