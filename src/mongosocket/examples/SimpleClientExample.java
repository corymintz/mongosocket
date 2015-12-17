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
        MongoSocketClient client = new MongoSocketClient(
            new MongoClient(),
            new MongoNamespace("test.test")
        );

       MongoSocket s = client.connect(600);
       OutputStream stream = s.getOutputStream();
       stream.write("Hello, World.".getBytes());
       stream.close();
    }
}
