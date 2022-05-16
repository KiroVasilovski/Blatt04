package de.dis;

/**
 * Client threads
 *
 */
public class Client extends Thread{

    String name;
    PersistenceManager pm;
    int page;

    public Client(String name, int page){

        this.name = name;
        this.page = page;
        pm = PersistenceManager.getInstance();
    }

    /**
     * Overriding run method of Thread class
     */
    @Override public void run() {

        int taid;

        while(true){

            //Client action
            taid= pm.beginTransaction();

            int b = (int)(Math.random()*(10)+1);

            for(int i = 1; i < b; i++) {
                int c = (int)(Math.random()*(10)+page);
                System.out.println(name + " b=" + b + "c=" + c);
                pm.write(taid, c, name);

                try{

                    Thread.sleep(100 + (int) (Math.random() * 100));

                }catch(InterruptedException e){

                    return;
                }
            }

            pm.commit(taid);

        }
    }
}

