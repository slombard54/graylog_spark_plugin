package org.scott.spark;

import com.google.inject.Provider;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * Created by slombard on 3/25/15.
 */
public class SparkProvider implements Provider<JavaStreamingContext> {
    private static final Logger LOG = LoggerFactory.getLogger(SparkProvider.class);
    private static JavaStreamingContext ssc = null;

    @Inject
    public SparkProvider() {
        if (ssc == null) {
            LOG.info("Creating Spark Context");
            LogManager.getLogger("org.apache.spark").setLevel(Level.ERROR);
            LogManager.getLogger("org.spark-project").setLevel(Level.ERROR);
            SparkConf conf = new SparkConf().setAppName("graylog").setMaster("local[*]");
            ssc = new JavaStreamingContext(conf, Durations.seconds(10));
            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                                public void run() {
                            LOG.info("streaming context stopped");
                            ssc.stop();
                        }
                    });
        }
    }
    @Override
    public JavaStreamingContext get() {
        return ssc;
    }
}
