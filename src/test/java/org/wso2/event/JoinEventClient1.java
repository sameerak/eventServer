package org.wso2.event;

import org.wso2.event.client.EventClient;
import org.wso2.event.server.StreamDefinition;

import java.util.Random;

/**
 * Created by sameerak on 6/7/14.
 */
public class JoinEventClient1 {


    public static void main(String[] args) throws Exception {

        String receiver = "localhost:7615";
        if (args.length != 0 && args[0] != null) {
            receiver = args[0];
        }
        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId("TestStream1");
        streamDefinition.addAttribute("att1", StreamDefinition.Type.INT);
        streamDefinition.addAttribute("att2", StreamDefinition.Type.FLOAT);
        streamDefinition.addAttribute("att3", StreamDefinition.Type.STRING);
        streamDefinition.addAttribute("att4", StreamDefinition.Type.FLOAT);
        streamDefinition.addAttribute("att5", StreamDefinition.Type.INT);

        EventClient eventClient = new EventClient(receiver, streamDefinition);


        Thread.sleep(1000);
        System.out.println("Start testing");
        Random random = new Random();

        for (int i = 0; i < 1000000000; i++) {
            Object[] event = new Object[]{random.nextInt(100), random.nextFloat(), "Abcdefghijklmnop" + random.nextLong(), random.nextFloat(), random.nextInt()};
            System.out.println("eventINT = " + event[0]);
            eventClient.sendEvent(event);
        }
    }
}
