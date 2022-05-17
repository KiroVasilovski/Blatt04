package de.dis.helper;

/**
 * Value for the Hashtable (Buffer)
 */
public record BufferEntry(int lsn, int taid, String data) {}