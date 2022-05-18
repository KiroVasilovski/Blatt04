package de.dis;

import de.dis.helper.BufferEntry;
import de.dis.logs.LogAction;
import de.dis.logs.LogStore;
import de.dis.pages.PageData;
import de.dis.pages.PageStore;

import java.io.*;
import java.util.*;

/**
 * Class for the Persistence Manager managing logging, access to user data for clients
 * and persisting user data to permanent storage.
 */
public class PersistenceManager {
    private final Hashtable<Integer, BufferEntry> _buffer = new Hashtable<>(); //Key is pageID
    private final LinkedList<Integer> _uncommittedTa = new LinkedList<>();
    private int _taid = 1;

    private static final int BUFFERSIZE = 5;

    static final private PersistenceManager singleton;

    static {
        try {
            singleton = new PersistenceManager();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Constructor: performs recovery if needed and clears the log file
     */
    private PersistenceManager() {
        // run recovery tool and add unrecoverable entries back into the buffer
        // TODO should unrecoverable entries be ignored?
//        Map<Integer, BufferEntry> unrecovered = RecoveryTool.run();
//        _buffer.putAll(unrecovered);
//
        LogStore.clear();
    }

    static public PersistenceManager getInstance() {
        return singleton;
    }

    /**
     * Starts a new transaction. The persistence manager creates a unique transaction ID and returns it to the client.
     */
    public int beginTransaction() {
        int taid = _taid;
        _taid += 1;
        _uncommittedTa.add(taid);

        try {
            LogStore.addEntry(taid, LogAction.TRANSACTION_BEGIN);
        } catch (IOException e) {
            System.out.println("An error occurred while writing to log-file.");
            e.printStackTrace();
        }

        return taid;
    }

    /**
     * Commits the transaction specified by the given transaction ID
     *
     * @param taid ID of the transaction to be committed
     */
    public void commit(int taid) {
        try {
            LogStore.addEntry(taid, LogAction.TRANSACTION_END);

            _uncommittedTa.remove((Integer) taid);
        } catch (IOException e) {
            System.out.println("An error occurred while writing to log-file.");
            e.printStackTrace();
        }
    }

    /**
     * Writes the given data with the given page ID on behalf of the given transaction to the buffer.
     * If the given page already exists, its content is replaced completely by the given data.
     *
     * @param taid   ID of the modifying transaction
     * @param pageid ID of the page being modified
     * @param data   Data to be written to the given page
     */
    public synchronized void write(int taid, int pageid, String data) {
        try {
            int lsn = LogStore.addEntry(taid, pageid, data);

            //write to buffer
            BufferEntry bufferEntry = new BufferEntry(lsn, taid, data);
            _buffer.put(pageid, bufferEntry);

            //check if buffer is full
            synchronized (this) {
                if (_buffer.size() > BUFFERSIZE) {
                    handleFullBuffer();
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred while writing to log-file.");
            e.printStackTrace();
        }
    }

    /**
     * Handles the propagation of changes to permanent DB if the size of the buffer reached the predetermined maximum.
     */
    private void handleFullBuffer() {
        LinkedList<Integer> toDelete = new LinkedList<>();

        //Check the buffer for committed TAs, get corresponding taids from hashtable, double check with list
        for (Map.Entry<Integer, BufferEntry> entry : _buffer.entrySet()) {
            int pageId = entry.getKey();
            BufferEntry bufEntry = entry.getValue();

            if (!_uncommittedTa.contains(bufEntry.taid())) {
                //write user data to permanent DB
                try {
                    PageStore.write(pageId, new PageData(bufEntry.lsn(), bufEntry.data()));
//                    PageStore.write(pageId, null);

                    System.out.printf("page %d written to permanent DB\n", pageId);

                    //delete corresponding page in buffer
                    toDelete.add(pageId);
                } catch (IOException e) {
                    System.out.println("An error occurred while trying to write to permanent DB.");
                    e.printStackTrace();
                }
            }

//            else {
            //brauchen wir nicht, weil buffer nicht auf 5 begrenzt ist, sodass bei commit die entsprechenden pages propagiert werden können
//            }

        }

        for (int pageid : toDelete) {
            _buffer.remove(pageid);
        }
    }
}