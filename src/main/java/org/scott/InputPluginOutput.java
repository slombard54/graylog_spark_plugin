package org.scott;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.google.common.collect.Lists;

import com.google.inject.name.Named;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.scott.spark.SparkDriverService;
import scala.Tuple2;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.streams.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by slombard on 3/17/15.
 */
public class InputPluginOutput implements MessageOutput  {
    private static final Logger LOG = LoggerFactory.getLogger(InputPluginOutput.class);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final SparkDriverService driver;
    private final Configuration configuration;
    private final ArrayBlockingQueue<Message> messages;


    @AssistedInject
    public InputPluginOutput(@Assisted Stream stream, @Assisted Configuration config, SparkDriverService sparkDrive, ArrayBlockingQueue<Message> a) throws MessageOutputConfigurationException {
        LOG.info("Initializing");

        configuration = config;
        driver = sparkDrive;
        messages = a;
        isRunning.set(true);



    }

    @Override
    public void stop() {
        isRunning.set(false);
        LOG.info("Stopping");
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void write(Message message) throws Exception {
        //LOG.info("{} {}", configuration.getString("prefix"), message);
        //LOG.info("writing message");
        ServerSocket serverSocket = null;
        Socket socket = null;
        ObjectOutputStream outputStream = null;
        try
        {
            serverSocket = new ServerSocket(45678);
            socket = serverSocket.accept();
            Map fields = message.getFields();
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeObject(fields);
            socket.close();
            //driver.messages.put(message);
            //messages.put(message);
            //driver.writeObject(message);
        }
        catch (Exception e)
        {
            LOG.error("{}", e);
        }



    }

    @Override
    public void write(List<Message> messages) throws Exception {

        LOG.info("writing messages");

    }




    @FactoryClass
    public interface Factory extends MessageOutput.Factory<InputPluginOutput> {
        @Override
        InputPluginOutput create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("Smart Output", false, "", "An output writing every message to STDOUT.");
        }
    }


    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            ConfigurationRequest configurationRequest = new ConfigurationRequest();
            configurationRequest.addField(new TextField("prefix", "Prefix", "Writing message: ", "How to prefix the message before logging it", ConfigurationField.Optional.OPTIONAL));
            return configurationRequest;
        }
    }



}
