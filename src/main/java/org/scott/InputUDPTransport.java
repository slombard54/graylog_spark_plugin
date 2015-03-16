package org.scott;


import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.eventbus.EventBus;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.transports.RandomMessageTransport;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.transports.Transport;


/**
 * Created by slombard on 3/12/15.
 */
public class InputUDPTransport extends RandomMessageTransport{
    @AssistedInject
    public InputUDPTransport(@Assisted Configuration configuration,
                               EventBus eventBus,
                              ObjectMapper objectMapper){
        super( configuration,eventBus,objectMapper);
    }


    @FactoryClass
    public interface Factory extends Transport.Factory<InputUDPTransport> {
        @Override
        InputUDPTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends RandomMessageTransport.Config {

    }
}
