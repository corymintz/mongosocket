package mongosocket.examples;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import mongosocket.MongoSocket;
import mongosocket.MongoSocketServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleServerExample {

    public static void main(String[] args) {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketServer server = new MongoSocketServer(
                new MongoClient(),
                new MongoNamespace("test.test")
        );

        final AtomicInteger connections = new AtomicInteger(0);
        server.setCallback((MongoSocket pMongoSocket) -> {
            int connection = connections.incrementAndGet();
            System.out.println("NEW CONNECTION: (" + connection + ")");

            try {
                InputStream stream = pMongoSocket.getInputStream();
                byte[] buffer = new byte[1024];

                while (true) {
                    int read = stream.read(buffer);
                    if (read == 0) {
                        break;
                    }

                    System.out.println("(" + connection + ") " + new String(buffer, 0, read));
                }
            } catch (IOException e) {}
        });

        server.start();
    }
}
