package org.wso2.event;

import org.wso2.event.client.EventClient;
import org.wso2.event.server.EventServer;
import org.wso2.event.server.EventServerConfig;
import org.wso2.event.server.StreamDefinition;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.IOException;

/**
 * Created by sameerak on 6/7/14.
 */
public class EventJoinProcessor {
    private static SiddhiManager siddhiManager;
    private static volatile long count=0;

    public static void main(String[] args) throws Exception {

        int port = 7614;
        final int port1;
        String receiver = "localhost:7613";
        if (args.length != 0 && args[0] != null) {
            port = Integer.parseInt(args[0]);
        }

        if (args.length != 0 && args[1] != null) {
            port1 = Integer.parseInt(args[1]);
        }
        else
        port1 = 7615;

        if (args.length != 0 && args[2] != null) {
            receiver = args[2];
        }

        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setStreamId("TestStream");
        streamDefinition.addAttribute("att1", StreamDefinition.Type.INT);
        streamDefinition.addAttribute("att2", StreamDefinition.Type.FLOAT);
        streamDefinition.addAttribute("att3", StreamDefinition.Type.STRING);
        streamDefinition.addAttribute("att4", StreamDefinition.Type.INT);

        final StreamDefinition streamDefinition1 = new StreamDefinition();
        streamDefinition1.setStreamId("TestStream1");
        streamDefinition1.addAttribute("att1", StreamDefinition.Type.INT);
        streamDefinition1.addAttribute("att2", StreamDefinition.Type.FLOAT);
        streamDefinition1.addAttribute("att3", StreamDefinition.Type.STRING);
        streamDefinition1.addAttribute("att4", StreamDefinition.Type.FLOAT);
        streamDefinition1.addAttribute("att5", StreamDefinition.Type.INT);

        final EventClient eventClient = new EventClient(receiver, streamDefinition); //creating a connection to the output receiver

        siddhiManager = new SiddhiManager();
        StringBuilder stringBuilder = new StringBuilder();

        String attributeStr = streamDefinition.getAttributeList().get(0).getName()+" "+streamDefinition.getAttributeList().get(0).getType().toString().toLowerCase();
        for(int i=1; i<streamDefinition.getAttributeList().size();i++){
            attributeStr += "," + streamDefinition.getAttributeList().get(i).getName() + " " + streamDefinition.getAttributeList().get(i).getType().toString().toLowerCase();
        }

        String attributeStr1 = streamDefinition1.getAttributeList().get(0).getName()+" "+streamDefinition1.getAttributeList().get(0).getType().toString().toLowerCase();
        for(int i=1; i<streamDefinition1.getAttributeList().size();i++){
            attributeStr1 += "," + streamDefinition1.getAttributeList().get(i).getName() + " " + streamDefinition1.getAttributeList().get(i).getType().toString().toLowerCase();
        }

        siddhiManager.defineStream("define stream "+streamDefinition.getStreamId()+" ( "+attributeStr+" )");
        siddhiManager.defineStream("define stream "+streamDefinition1.getStreamId()+" ( "+attributeStr1+" )");

        siddhiManager.addQuery("from TestStream#window.length(2000)  as t join " +
                "TestStream1#window.time(500) as n " +
                "on t.att1 == n.att1 " +
                "select t.att1, t.att2, t.att3, t.att4 " +
                "insert into StockQuote;");

//        siddhiManager.addQuery("from  TestStream1 [att1>50]" +
//                "select att1, att2, att3, att5 "+
//                "insert into StockQuote ;");

        siddhiManager.addCallback("StockQuote", new org.wso2.siddhi.core.stream.output.StreamCallback() {
            @Override
            public void receive(Event[] events) {
//                EventPrinter.print(events);
//                count++;
                //send processed events to the output receiver
                //get event data from the event array and send them individually to the reciever
                for(Event event : events){
                    try {
                        eventClient.sendEvent(event.getData());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });




        new Thread(new Runnable() {
            @Override
            public void run() {
        try {
            EventServer eventServer1 = new EventServer(new EventServerConfig(port1), streamDefinition1, new org.wso2.event.server.StreamCallback() {
                @Override
                public void receive(Object[] event) {
    //                System.out.println("test");
                    InputHandler inputHandler = siddhiManager.getInputHandler("TestStream1");
                    if(inputHandler != null) {
                        try {
                            inputHandler.send(event);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }else{
                        System.out.println("Could not retrieve stream handler");
                        throw new RuntimeException("Could not retrieve stream handler");
                    }

                }
            });
            eventServer1.start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

            }
        }).start();

        EventServer eventServer = new EventServer(new EventServerConfig(port), streamDefinition, new org.wso2.event.server.StreamCallback() {
            @Override
            public void receive(Object[] event) {
//                System.out.println("test");
                InputHandler inputHandler = siddhiManager.getInputHandler("TestStream");
                if(inputHandler != null) {
                    try {
                        inputHandler.send(event);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else{
                    System.out.println("Could not retrieve stream handler");
                    throw new RuntimeException("Could not retrieve stream handler");
                }

            }
        });

        eventServer.start();

        Thread.sleep(10000);
    }
}
