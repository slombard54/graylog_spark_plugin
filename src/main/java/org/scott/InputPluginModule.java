package org.scott;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkContext;
import org.graylog2.plugin.PluginConfigBean;
import org.graylog2.plugin.PluginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.streaming.api.java.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Extend the PluginModule abstract class here to add you plugin to the system.
 */
public class InputPluginModule extends PluginModule {
    private static final Logger LOG = LoggerFactory.getLogger(InputPluginModule.class);
    /**
     * Returns all configuration beans required by this plugin.
     *
     * Implementing this method is optional. The default method returns an empty {@link Set}.
     */
    @Override
    public Set<? extends PluginConfigBean> getConfigBeans() {
        return Collections.emptySet();
    }

    @Override
    protected void configure() {
        /*
         * Register your plugin types here.
         *
         * Examples:
         *
         * addMessageInput(Class<? extends MessageInput>);
         * addMessageFilter(Class<? extends MessageFilter>);
         * addMessageOutput(Class<? extends MessageOutput>);
         * addPeriodical(Class<? extends Periodical>);
         * addAlarmCallback(Class<? extends AlarmCallback>);
         * addInitializer(Class<? extends Service>);
         * addRestResource(Class<? extends PluginRestResource>);
         *
         *
         * Add all configuration beans returned by getConfigBeans():
         *
         * addConfigBeans();
         */

        //SparkContext con = new SparkContext("local[4]","graylog","~/Spark-1.2.1/");
        SparkConf conf = new SparkConf().setAppName("graylog").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));


        LOG.info("test {}", Arrays.toString(Thread.currentThread().getStackTrace()));
        addTransport("Input-transport",InputUDPTransport.class);
        addMessageInput(InputPlugin.class);
    }
}
