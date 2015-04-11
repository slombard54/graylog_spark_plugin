package org.scott.Tasks;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by slombard on 3/29/15.
 */
public class MessageTask implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageTask.class);


    public void messageRun(JavaDStream<Map> inStream) {


            //JavaDStream<Map> winStream = inStream.window(Durations.seconds(20));
            inStream.print();
            //winStream.print();
            //System.out.print("This is a test!");
            //LOG.info("messageRun funtions ran.");

            //inStream.map()

    }

}

