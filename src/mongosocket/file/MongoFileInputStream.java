package mongosocket.file;

import mongosocket.MongoSocket;
import mongosocket.MongoSocketClient;
import mongosocket.MongoSocketConnectFailedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MongoFileInputStream extends InputStream {

    InputStream _stream;

    public MongoFileInputStream(MongoSocketClient pClient, String pFile) throws MongoSocketConnectFailedException, IOException {
        MongoSocket socket = pClient.connect(5, 5);
        OutputStream s = socket.getOutputStream();
        try {
            s.write(pFile.getBytes());
            _stream = socket.getInputStream();
        } finally {
            s.close();
        }
    }

    @Override
    public int read() throws IOException {
        return _stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return _stream.read(b, off, len);
    }

    @Override
    public void close() throws  IOException {
        _stream.close();
    }

}
