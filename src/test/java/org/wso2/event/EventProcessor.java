package org.wso2.event;

import org.wso2.event.client.EventClient;
import org.wso2.event.server.EventServer;
import org.wso2.event.server.EventServerConfig;
import org.wso2.event.server.StreamCallback;
import org.wso2.event.server.StreamDefinition;

/**
 * Created by sameerak on 6/5/14.
 */
public class EventProcessor {

    public static void main(String[] args) throws Exception {

        final StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId("TestStream");
        streamDefinition.addAttribute("att1", StreamDefinition.Type.INT);
        streamDefinition.addAttribute("att2", StreamDefinition.Type.FLOAT);
        streamDefinition.addAttribute("att3", StreamDefinition.Type.STRING);
        streamDefinition.addAttribute("att4", StreamDefinition.Type.INT);
        final EventClient eventClient = new EventClient("localhost:7613", streamDefinition);

        EventServer eventServer = new EventServer(new EventServerConfig(7612), streamDefinition, new StreamCallback() {
            @Override
            public void receive(Object[] event){
                try {
                    eventClient.sendEvent(event);
                } catch (Exception e) {
                    e.printStackTrace();
                }

//                System.out.println(Arrays.deepToString(event));
            }
        });

        eventServer.start();

        Thread.sleep(10000);
    }
}
