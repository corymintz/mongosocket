package mongosocket.examples;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import mongosocket.MongoSocket;
import mongosocket.MongoSocketClient;
import mongosocket.MongoSocketConnectFailedException;

import java.io.IOException;
import java.io.OutputStream;

public class SimpleClientExample {

    public static void main(String[] args) throws MongoSocketConnectFailedException, IOException {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketClient client = new MongoSocketClient(
                new MongoClient(),
                new MongoNamespace("test.test")
        );

        for(int i = 0; i < 25; i++) {
            Runnable task = () -> {
                try {
                    MongoSocket s = client.connect(600);
                    OutputStream stream = s.getOutputStream();
                    stream.write("Hello, World.".getBytes());
                    stream.close();
                } catch (Exception e) {}
            };

            new Thread(task).start();
        }
    }
}
