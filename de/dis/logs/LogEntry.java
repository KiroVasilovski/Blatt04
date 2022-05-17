package de.dis.logs;

public record LogEntry(int lsn, int taid, int pageid, String data) {}
