package mongosocket.examples;

import com.mongodb.MongoNamespace;
import mongosocket.MongoSocketClient;
import mongosocket.MongoSocketConnectFailedException;
import mongosocket.file.MongoFileInputStream;
import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class FileClientExample
{
    public static void main(String[] args) throws MongoSocketConnectFailedException, IOException {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketClient client = new MongoSocketClient(new MongoNamespace("test.test"));
        MongoFileInputStream stream = new MongoFileInputStream(client, args[0]);

        System.out.print(new String(IOUtils.toByteArray(stream)));
    }
}
