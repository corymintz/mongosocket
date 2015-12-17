package mongosocket.examples;

import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import mongosocket.MongoSocketClient;
import mongosocket.MongoSocketConnectFailedException;
import mongosocket.file.MongoFileInputStream;

import java.io.IOException;

public class FileClientExample
{
    public static void main(String[] args) throws MongoSocketConnectFailedException, IOException {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketClient client = new MongoSocketClient(
                new MongoClient(),
                new MongoNamespace("test.test")
        );

        MongoFileInputStream s = new MongoFileInputStream(client, args[0]);

        byte[] buffer = new byte[1024];
        while (true) {
            int read = s.read(buffer);
            if (read == 0) {
                break;
            }

            System.out.println(new String(buffer, 0, read));
        }
    }
}
