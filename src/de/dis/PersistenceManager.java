package de.dis;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;

import de.dis.helper.Value;

/**
 * Class for the Persistence Manager managing logging, access to user data for clients
 * and persisting user data to permanent storage.
 *
 */
public class PersistenceManager {

    private final File _logfile;
    private final Hashtable<Integer, Value> _buffer = new Hashtable<>(); //Key is pageID
    private int _lsn = 1;
    private int _taid = 1;
    private final LinkedList<Integer> _uncommittedTa = new LinkedList<>();
    private static final int BUFFERSIZE = 5;

    static final private PersistenceManager singleton;
    static {
        try {
            singleton = new PersistenceManager();
        }
        catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Constructor: creates new logfile/overrides existing one
     */
    private PersistenceManager() {
        _logfile = new File("logfile.txt");
        _logfile.delete();
        try {
            _logfile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static public PersistenceManager getInstance() {
        return singleton;
    }


    /**
     * Starts a new transaction. The persistence manager creates a unique transaction ID and returns it to the client.
     */
    public int beginTransaction(){

        //create unique TAID
        int taid = _taid;
        _taid+=1;
        _uncommittedTa.add(taid);

        //write into log-file
        try {
            FileWriter filewriter = new FileWriter(_logfile, true);
            filewriter.write( (_lsn++) +"," + taid + ", BOT\n");
            filewriter.close();
            //_lsn+=1;

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
    public void commit(int taid){
        //write into log-file
        try {
            FileWriter filewriter = new FileWriter(_logfile, true);
            filewriter.write( (_lsn++) + "," + taid + ", EOT\n");
            filewriter.close();
            //_lsn+=1;
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
     * @param taid ID of the modifying transaction
     * @param pageid ID of the page being modified
     * @param data Data to be written to the given page
     */
    public synchronized void write(int taid, int pageid, String data){
        //write into log-file
        try {
            int lsn = _lsn++;
            FileWriter filewriter = new FileWriter(_logfile, true);
            filewriter.write( lsn + "," + taid + "," + pageid + "," + data + "\n");
            filewriter.close();
            //_lsn+=1;

            //write to buffer
            Value value = new Value(lsn, taid, data);
            _buffer.put(pageid, value);

            //check if buffer is full
            synchronized(this) {
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
     * Handles the propagation of changes to permanent DB if the size of the buffer reached the predetermined maximum
     */
    private void handleFullBuffer() {

        LinkedList<Integer> toDelete = new LinkedList<>();

        //Check the buffer for committed TAs, get corresponding taids from hashtable, double check with list

        for( Map.Entry<Integer, Value> entry : _buffer.entrySet() ){

            int pageId = entry.getKey();
            Value bufEntry = entry.getValue();

            if(!_uncommittedTa.contains(bufEntry._taID)) {

                //write user data to permanent DB
                try {

                    //Create new File
                    File textFile = new File(pageId + ".txt");
                    textFile.createNewFile();

                    //write into file
                    FileWriter filewriter = new FileWriter(textFile);
                    filewriter.write(bufEntry._lsn + "," + bufEntry._data);
                    filewriter.close();

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

        for ( int pageid : toDelete) {
            _buffer.remove(pageid);
        }

    }

}
