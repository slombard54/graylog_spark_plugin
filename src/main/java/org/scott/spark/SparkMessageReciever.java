package org.scott.spark;

/**
 * Created by slombard on 3/30/15.
 */

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.ObjectInputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Map;




public class SparkMessageReciever extends Receiver<Map> {
    private static final Logger LOG = LoggerFactory.getLogger(SparkMessageReciever.class);
    public SparkMessageReciever() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_AND_DISK_2();
    }
    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {

    }

    private void receive() {

        Socket socket;
        Map userInput;

        try {
            // connect to the server
            socket = new Socket("localhost", 45678);

            ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = (Map)reader.readObject()) != null) {
                LOG.info("Received data {}", userInput);
                store(userInput);
            }
            reader.close();
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }


}