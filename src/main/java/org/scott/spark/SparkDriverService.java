package org.scott.spark;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.scott.Tasks.MessageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by slombard on 3/26/15.
 */
@Singleton
public class SparkDriverService extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(SparkDriverService.class);
    private final JavaStreamingContext ssc;
    private final MessageTask mt;
    public final ArrayBlockingQueue<Map> messages;
    private Thread messageserver;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @Inject
    public SparkDriverService(JavaStreamingContext context, ArrayBlockingQueue<Map> a) {
        LOG.info("Get Spark Context from provider");
        ssc = context;
        mt = new MessageTask();
        messages = a;
        isRunning.set(true);
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void run() throws Exception {
        LOG.info("Creating Spark Function");
        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream
        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
        // Create and push some RDDs into the queue
        List<Integer> list = Lists.newArrayList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }

        for (int i = 0; i < 30; i++) {
            rddQueue.add(ssc.sparkContext().parallelize(list));

        }
        messageserver = new Thread()  {
            @Override public void run() {
                send();
            }
        };
        messageserver.start();
        LOG.info("{}", messageserver);
        // Create the QueueInputDStream and use it do some processing
        //JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
        //mt.sparkrun(inputStream);

        //JavaReceiverInputDStream<Map> messageStream = ssc.receiverStream(new SparkMessageReciever());
        JavaReceiverInputDStream<Map> messageStream = ssc.socketStream("localhost", 45678, SparkFunctions.streamToMapConverter, StorageLevel.MEMORY_AND_DISK_SER_2());
        mt.messageRun(messageStream);
        LOG.info("Completed Spark Function Creation");

        LOG.info("Start Spark Streaming Context");
        try {
            //messages.put(new Message("Test Message","test", DateTime.now()));
            ssc.start();
            ssc.awaitTermination();
        } catch (Exception e){
            LOG.error("Spark Error");
            ssc.stop();
        }
        LOG.info("Start Spark Streaming Context awaitTermination Returned");
    }

    @Override
    protected void triggerShutdown() {
        LOG.info("Spark Streaming Context Stop");
        ssc.stop();
    }

    @Override
    protected void shutDown() throws Exception {
        isRunning.set(false);
        ssc.stop(true);
        LOG.info("Spark Streaming Driver shutdown");
    }


    private void send() {

            while (isRunning.get()) {

                try(ServerSocket serverSocket = new ServerSocket(45678);
                    Socket socket = serverSocket.accept()) {


                    try (ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream())) {
                        while (isRunning.get()) {
                            // added sleep to fix issue with socket timing
                            if (messages.isEmpty()) {
                                LOG.trace("No messages to send receiver waiting");
                                Thread.sleep(1000);

                            } else {
                                outputStream.writeObject(messages.poll());
                                LOG.trace("Object written to receiver");
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Message Sender exception", e);
                }
            }
    }

}