package org.scott.spark;

import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by slombard on 3/25/15.
 */
@Singleton
public class RecieverBufferProvider implements Provider<ArrayBlockingQueue<Map>> {
    private static final Logger LOG = LoggerFactory.getLogger(RecieverBufferProvider.class);
    private static ArrayBlockingQueue<Map> messages = null;

    @Inject
    public RecieverBufferProvider() {
        if (messages == null) {
          messages = new ArrayBlockingQueue<>(2048);
        }
    }
    @Override
    public ArrayBlockingQueue<Map> get() {
        return messages;
    }
}
