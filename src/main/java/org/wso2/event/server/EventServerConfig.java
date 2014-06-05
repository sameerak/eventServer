package org.wso2.event.server;

/**
 * Created by suho on 6/5/14.
 */
public class EventServerConfig {

    private int numberOfThreads=10;
    private int port;

    public EventServerConfig(int port) {
        this.port = port;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public int getPort() {
        return port;
    }
}
