package org.scott.spark;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by slombard on 4/1/15.
 */
public final class SparkFunctions {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFunctions.class);
    private SparkFunctions(){}

    public static Function streamToMapConverter = new Function<InputStream, Iterable<Map>>()
    {

        public Iterable<Map> call(InputStream is)
        {

            class IterableClass implements Iterator<Map>, Iterable<Map>, Serializable
            {
                private InputStream is;

                private ObjectInputStream os = null;

                private boolean hasNext = false;
                private boolean done = false;
                private Map nextValue = null;

                IterableClass(InputStream is)
                {
                    this.is = is;
                    try {

                        this.os = new ObjectInputStream(new BufferedInputStream(is));

                    } catch (IOException e) {

                        LOG.error("Object Input Stream IOException", e);

                    }
                }

                private void getNext()
                {
                    try {
                        try {

                            nextValue = (Map) os.readObject();

                        } catch (EOFException e) {
                            // Error occurs when otherside of connection is close
                            // happens when shutting down ignore
                            done = true;
                            //TODO fix exception when shutting down if possible
                            LOG.trace("EOFException", e);
                        } catch (Exception e) {
                            done = true;
                            LOG.error("Generic Exception", e);
                        }

                        if (nextValue == null) {
                            done = true;
                        }

                    } catch (Exception e) {
                        LOG.error("getNext Generic Excpetion", e);
                        throw new RuntimeException(e);
                    }
                    hasNext = true;
                }

                @Override
                public boolean hasNext()
                {
                    if (!done) {
                        if (!hasNext) {
                            getNext();
                            if (done) {
                                LOG.debug(">>> streamToMapconverter hasNext DONE");
                                if (os != null) {
                                    try {
                                        LOG.debug(">>> streamToMapconverter hasNext DONE CLOSING OS");
                                        os.close();
                                    } catch (IOException e) {
                                        LOG.error("hasNext IOException", e);
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        }
                    }
                    return !done;
                }

                @Override
                public Map next()
                {
                    if (done) {
                        LOG.debug("steamToMapConverter End of InputStream");
                        throw new NoSuchElementException("End of InputStream");
                    }
                    if (!hasNext) {
                        getNext();
                    }
                    hasNext = false;
                    return nextValue;
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Iterator<Map> iterator()
                {
                    return this;
                }
            }

            IterableClass myIterable = new IterableClass(is);
            LOG.debug(">>> streamToMapConverter returning myIterable");

            return myIterable;
        }
    };
}
