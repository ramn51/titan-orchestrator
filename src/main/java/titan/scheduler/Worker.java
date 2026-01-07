package titan.scheduler;

import java.util.ArrayList;
import java.util.List;

public class Worker {
    private final String host;
    private final int port;
    private final List<String> capabilities;

    // Mutable State
    private long lastSeen;
    private int currentLoad;
    private int maxCap;
    public String currentJobId = null;
    // This will replace maxCap
    public static final int MAX_SLOTS = 4;
    private long idleStartTime = -1;
    private boolean isPermanent;


    public Worker(String host, int port, List<String> capabilities, boolean isPermanent) {
        this.host = host;
        this.port = port;
        this.capabilities = (capabilities == null) ? new ArrayList<>() : capabilities;
        this.lastSeen = System.currentTimeMillis();
        this.currentLoad = 0;
        maxCap = 4;
        this.isPermanent = isPermanent;
        this.idleStartTime = System.currentTimeMillis();
    }

    public synchronized void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    public boolean isPermanent() {
        return isPermanent;
    }

    public synchronized void setCurrentLoad(int load) {
        this.currentLoad = load;
        if (load == 0) {
            if (this.idleStartTime == -1) {
                this.idleStartTime = System.currentTimeMillis();
            }
        } else {
            // Reset this once it gets a work immediately.
            this.idleStartTime = -1;
        }
    }

    public synchronized void setMaxCap(int maxCap){
        this.maxCap = maxCap;
    }

    public synchronized int getMaxCap(){
        return this.maxCap;
    }

    public long getIdleDuration() {
        if (idleStartTime == -1) return 0;
        return System.currentTimeMillis() - idleStartTime;
    }

    public synchronized int getCurrentLoad() {
        return currentLoad;
    }

    synchronized public void incrementCurrentLoad(){
        this.currentLoad++;
    }

    synchronized public void decrementCurrentLoad(){
        if(this.currentLoad > 0){
            this.currentLoad--;
        }

        if (this.currentLoad == 0) {
            this.idleStartTime = System.currentTimeMillis();
        }
    }

    public synchronized boolean isSaturated(){
        return getCurrentLoad() >= maxCap;
    }

    public String host() { return host; }
    public int port() { return port; }
    public long lastSeen() { return lastSeen; }
    public List<String> capabilities() { return capabilities; }

    @Override
    public String toString() {
        return host + ":" + port + " [Load=" + currentLoad + "]";
    }
}