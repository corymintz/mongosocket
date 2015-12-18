package mongosocket.examples;

import com.mongodb.MongoNamespace;
import mongosocket.MongoSocket;
import mongosocket.MongoSocketServer;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleServerExample {

    public static void main(String[] args) {
        java.util.logging.LogManager.getLogManager().reset();
        final AtomicInteger connections = new AtomicInteger(0);

        MongoSocketServer server = new MongoSocketServer(new MongoNamespace("test.test"));
        server.setCallback((MongoSocket pMongoSocket) -> {
            int connection = connections.incrementAndGet();
            System.out.println("(" + connection + ") New Connection");

            try {
                byte[] bytes = IOUtils.toByteArray(pMongoSocket.getInputStream());
                System.out.println("(" + connection + ") Data: " + new String(bytes));
            } catch (IOException e) {}
        });

        server.start();
    }
}
