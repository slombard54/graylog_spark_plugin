package org.scott;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * Implement the PluginMetaData interface here.
 */
public class InputPluginMetaData implements PluginMetaData {
    @Override
    public String getUniqueId() {
        return "org.scott.InputPluginPlugin";
    }

    @Override
    public String getName() {
        return "InputPlugin";
    }

    @Override
    public String getAuthor() {
        return "Scott Lombard";
    }

    @Override
    public URI getURL() {
        // TODO Insert correct plugin website
        return URI.create("https://www.github.com/slombard54");
    }

    @Override
    public Version getVersion() {
        return new Version(0, 1, 0);
    }

    @Override
    public String getDescription() {
        // TODO Insert correct plugin description
        return "Plugin to test the sending data to graylog";
    }

    @Override
    public Version getRequiredVersion() {
        return new Version(1, 0, 0);
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.emptySet();
    }
}
