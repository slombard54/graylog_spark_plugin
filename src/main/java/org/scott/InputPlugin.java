package org.scott;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.graylog2.inputs.codecs.RandomHttpMessageCodec;
import org.graylog2.inputs.codecs.RawCodec;
import org.graylog2.inputs.transports.RandomMessageTransport;
import org.graylog2.inputs.transports.UdpTransport;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;



import javax.inject.Inject;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class InputPlugin extends MessageInput {

    private static final String NAME = "Scott Plugin Test";

    @AssistedInject
    public InputPlugin(MetricRegistry metricRegistry,
                       @Assisted final Configuration configuration,
                       final InputUDPTransport.Factory transportFactory,
                       final RandomHttpMessageCodec.Factory codecFactory,
                       LocalMetricRegistry localRegistry,
                       Config config,
                       Descriptor descriptor,
                       ServerStatus serverStatus) {
        super(metricRegistry, configuration, transportFactory.create(configuration), localRegistry, codecFactory.create(configuration), config, descriptor, serverStatus);
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<InputPlugin> {
        @Override
        InputPlugin create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();

        Duration a = Durations.seconds(1);
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    @ConfigClass
    public static class Config extends MessageInput.Config {
        @Inject
        public Config(InputUDPTransport.Factory transport, RandomHttpMessageCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
