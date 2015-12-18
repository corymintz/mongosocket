package mongosocket.examples;

import com.mongodb.MongoNamespace;
import mongosocket.MongoSocketClient;
import mongosocket.MongoSocketConnectFailedException;
import mongosocket.file.MongoFileInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;

import java.io.FileOutputStream;
import java.io.IOException;

public class FileClientExample
{
    public static void main(String[] args) throws MongoSocketConnectFailedException, IOException {
        java.util.logging.LogManager.getLogManager().reset();

        MongoSocketClient client = new MongoSocketClient(new MongoNamespace("test.test"));

        MongoFileInputStream inStream = new MongoFileInputStream(client, args[0]);
        CountingInputStream cInStream = new CountingInputStream(inStream);
        FileOutputStream outStream = new FileOutputStream(args[1]);

        long start = System.currentTimeMillis();
        IOUtils.copy(cInStream, outStream);
        outStream.close();
        long timeTaken = System.currentTimeMillis() - start;

        System.out.println(String.format("Transferred %s bytes in %s ms (%.2f MB/sec)",
            cInStream.getByteCount(),
            timeTaken,
            ((double)cInStream.getByteCount() / 1000 / 1000) / (timeTaken / 1000L)));
    }
}
