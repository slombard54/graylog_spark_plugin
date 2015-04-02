package org.scott;

import com.google.inject.TypeLiteral;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.PluginConfigBean;
import org.graylog2.plugin.PluginModule;
import org.scott.spark.RecieverBufferProvider;
import org.scott.spark.SparkDriverService;
import org.scott.spark.SparkProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

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
        try {
            //TODO: Make path relative or add independent classloader
            addPath("/home/slombard/Documents/graylog/inputplugin/target/inputplugin-1.0.0-SNAPSHOT.jar");
        }
        catch (Exception e) {
            LOG.error("Exception: {}",e);
        }

        LogManager.getLogger("org.scott").setLevel(Level.TRACE);
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            LOG.info("Jar files: {}",(url.getFile()));
        }

        //LOG.info("test {}", Arrays.toString(Thread.currentThread().getStackTrace()));


        bind(JavaStreamingContext.class).toProvider(SparkProvider.class);
        bind(new TypeLiteral<ArrayBlockingQueue<Map>>(){}).toProvider(RecieverBufferProvider.class);

        addInitializer(SparkDriverService.class);
        addMessageInput(InputPlugin.class);
        addMessageOutput(InputPluginOutput.class);
    }

    public static void addPath(String s) throws Exception {
        File f = new File(s);
        URL u = f.toURI().toURL();
        URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class urlClass = URLClassLoader.class;
        Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
        method.setAccessible(true);
        method.invoke(urlClassLoader, new Object[]{u});
    }
}
