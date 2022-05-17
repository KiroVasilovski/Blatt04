package de.dis.logs;

public enum LogAction {
    TRANSACTION_BEGIN("BOT"),
    TRANSACTION_END("EOT");

    private final String code;

    private LogAction(String code) {
        this.code = code;
    }

    public String code() {
        return this.code;
    }
}
