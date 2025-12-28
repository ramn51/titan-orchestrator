import scheduler.Scheduler;

public class TitanMaster {
    public static void main(String[] args) {
        // Start Scheduler on port 9090
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();

        scheduler.startAutoScaler();

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}