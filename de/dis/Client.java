package de.dis;

/**
 * Client threads
 */
public class Client extends Thread {

    String name;
    PersistenceManager pm;
    int firstPage;

    public Client(String name, int page) {
        this.name = name;
        this.firstPage = page;
        pm = PersistenceManager.getInstance();
    }

    /**
     * Overriding run method of Thread class
     */
    @Override
    public void run() {
        int taid;

        while (true) {
            //Client action
            taid = pm.beginTransaction();

            int writeCount = (int) (Math.random() * (10) + 1);

            for (int i = 1; i < writeCount; i++) {
                int page = (int) (Math.random() * (10) + firstPage);
                System.out.printf("%s -> page %d (write %d of %d)\n", name, page, i, writeCount);
                pm.write(taid, page, name);

                try {
                    Thread.sleep(100 + (int) (Math.random() * 100));
                } catch (InterruptedException e) {
                    return;
                }
            }

            pm.commit(taid);
        }
    }
}

