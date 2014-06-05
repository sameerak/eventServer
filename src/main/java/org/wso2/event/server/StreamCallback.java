package org.wso2.event.server;

/**
 * Created by suho on 6/5/14.
 */
public interface StreamCallback {

    void receive(Object[] event);
}
