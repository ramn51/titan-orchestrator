package scheduler;

import java.util.ArrayList;
import java.util.Collections;
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


    public Worker(String host, int port, List<String> capabilities) {
        this.host = host;
        this.port = port;
        this.capabilities = (capabilities == null) ? new ArrayList<>() : capabilities;
        this.lastSeen = System.currentTimeMillis();
        this.currentLoad = 0;
        maxCap = 4;
    }

    public void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    public void setCurrentLoad(int load) {
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

    public long getIdleDuration() {
        if (idleStartTime == -1) return 0;
        return System.currentTimeMillis() - idleStartTime;
    }

    public int getCurrentLoad() {
        return currentLoad;
    }

    synchronized public void incrementCurrentLoad(){
        this.currentLoad++;
    }

    synchronized public void decrementCurrentLoad(){
        if(this.currentLoad > 0){
            this.currentLoad--;
        }
    }

    public boolean isSaturated(){
        return getCurrentLoad() == maxCap;
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