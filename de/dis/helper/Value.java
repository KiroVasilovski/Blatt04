package de.dis.helper;

/**
 * Value for the Hashtable (Buffer)
 */
public class Value {
    public int _taID;
    public int _lsn;
    public String _data;

    /**
     * Constructor
     * @param lsn LSN
     * @param taid Transaction ID
     * @param data User Data
     */
    public Value(int lsn, int taid, String data) {
        this._lsn = lsn;
        this._taID = taid;
        this._data = data;
    }
}

