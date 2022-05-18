package de.dis;

/**
 * Client threads
 */
public class Client extends Thread {
    private static final String[] DATA_OPTIONS = new String[]{
            "Java", "Python", "C++", "Rust", "TypeScript", "Go", "Ruby", "Haskell", "Kotlin", "Swift"
    };

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
            taid = pm.beginTransaction();

            int writeCount = (int) (Math.random() * (10) + 1);

            for (int i = 1; i < writeCount; i++) {
                int page = (int) (Math.random() * (10) + firstPage);
                String data = name + "-" + DATA_OPTIONS[(int) (Math.random() * DATA_OPTIONS.length)];

                System.out.printf("%s -> page %d (write %d of %d)\n", data, page, i, writeCount);
                pm.write(taid, page, data);

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

