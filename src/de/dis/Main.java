package de.dis;

public class Main {

    public static void main(String[] args) {
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
        } catch (InterruptedException var9) {
            var9.printStackTrace();
        }

        t1.interrupt();
        t2.interrupt();
        t3.interrupt();
        t4.interrupt();
        t5.interrupt();
    }
}
