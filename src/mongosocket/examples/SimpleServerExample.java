package mongosocket.examples;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import mongosocket.MongoSocket;
import mongosocket.MongoSocketServer;

import java.io.IOException;
import java.io.InputStream;

public class SimpleServerExample {

    public static void main(String[] args) {
        MongoSocketServer server = new MongoSocketServer(
                new MongoClient(),
                new MongoNamespace("test.test")
        );

        server.setCallback((MongoSocket pMongoSocket) -> {
            System.out.print("NEW CONNECTION: ");
            try {
                InputStream s = pMongoSocket.getInputStream();
                byte[] buffer = new byte[1024];

                while (true) {
                    int read = s.read(buffer);
                    if (read == 0) {
                        break;
                    }

                    System.out.print(new String(buffer, 0, read));
                }

                System.out.println();
            } catch (IOException e) {}
        });

        server.start();
    }
}
