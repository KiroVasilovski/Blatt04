package de.dis;

import de.dis.logs.LogStore;

public class Main {
    public static void main(String[] args) {
//        RecoveryTool.run(true);

        startClients();
    }

    /**
     * Start the clients and simulate database access.
     *
     * **WARNING**: deletes the old log file at the start.
     */
    private static void startClients() {
        Thread t1 = new Client("client1", 10);
        Thread t2 = new Client("client2", 20);
        Thread t3 = new Client("client3", 30);
        Thread t4 = new Client("client4", 40);
        Thread t5 = new Client("client5", 50);
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        t1.interrupt();
        t2.interrupt();
        t3.interrupt();
        t4.interrupt();
        t5.interrupt();
    }
}
