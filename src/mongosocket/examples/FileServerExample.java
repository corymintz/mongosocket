package mongosocket.examples;

import java.io.File;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import mongosocket.MongoSocketServer;
import mongosocket.file.MongoFileServer;

public class FileServerExample {

    public static void main(String[] args) {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketServer server = new MongoSocketServer(
                new MongoClient(),
                new MongoNamespace("test.test")
        );
        server.start();
        new MongoFileServer(server, new File("/tmp"));
    }
}
