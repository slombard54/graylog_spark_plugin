package org.scott.spark;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.graylog2.plugin.Message;
import org.joda.time.DateTime;
import org.scott.Tasks.MessageTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by slombard on 3/26/15.
 */
@Singleton
public class SparkDriverService extends AbstractExecutionThreadService {
    private static final Logger LOG = LoggerFactory.getLogger(SparkDriverService.class);
    private final JavaStreamingContext ssc;
    private final MessageTask mt;
    public final ArrayBlockingQueue<Message> messages;

    @Inject
    public SparkDriverService(JavaStreamingContext context, ArrayBlockingQueue<Message> a) {
        LOG.info("Get Spark Context from provider");
        ssc = context;
        mt = new MessageTask();
        messages = a;
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
        // Create the QueueInputDStream and use it do some processing
        //JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
        //mt.sparkrun(inputStream);

        JavaReceiverInputDStream<Map> messageStream = ssc.receiverStream(new SparkMessageReciever());
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
        LOG.info("Start Spark Streaming Context awaitTermination Return");
    }

    @Override
    protected void triggerShutdown() {
        LOG.info("Spark Streaming Context Stop");
        ssc.stop();
    }

    @Override
    protected void shutDown() throws Exception {
        ssc.stop(true);
        LOG.info("Spark Streaming Driver shutdown");
    }

    public void writeObject(Object o){
        try {

        } catch (Exception e){}
    }


}