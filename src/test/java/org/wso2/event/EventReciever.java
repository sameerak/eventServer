package org.wso2.event;

import org.wso2.event.server.EventServer;
import org.wso2.event.server.EventServerConfig;
import org.wso2.event.server.StreamCallback;
import org.wso2.event.server.StreamDefinition;

import java.util.Arrays;

public class EventReciever {

    private static volatile long count=0;
    private static volatile long start=System.currentTimeMillis();

    public static void main(String[] args) throws Exception {

        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId("TestStream");
        streamDefinition.addAttribute("att1", StreamDefinition.Type.INTEGER);
        streamDefinition.addAttribute("att2", StreamDefinition.Type.FLOAT);
        streamDefinition.addAttribute("att3", StreamDefinition.Type.STRING);
        streamDefinition.addAttribute("att4", StreamDefinition.Type.INTEGER);

        EventServer eventServer = new EventServer(new EventServerConfig(7613), streamDefinition, new StreamCallback() {
            @Override
            public void receive(Object[] event) {
                count++;
                if(count%1000==0){
                    long end=System.currentTimeMillis();
                    System.out.println("TP:"+(count*1000.0/(end-start)));
                    start=end;
                }
            }
        });

        eventServer.start();

        Thread.sleep(10000);
    }
}
