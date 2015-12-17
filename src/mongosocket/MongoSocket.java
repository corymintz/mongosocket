package mongosocket;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.types.Binary;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MongoSocket {

    private long _clientSequenceNum;
    private long _serverSequenceNum;

    private MongoCollection<Document> _sendCollection;
    private MongoCursor<Document> _receiveCursor;
    private MongoCollection<Document> _sendControlCollection;
    private MongoCollection<Document> _receiveControlCollection;

    public MongoSocket(MongoCollection<Document> pSendCollection,
                       MongoCollection<Document> pSendControlCollection,
                       MongoCursor<Document> pRceieveCursor,
                       MongoCollection<Document> pReceiveControlCollection,
                       long pClientSequenceNumber,
                       long pServerSequenceNumber) {
        _sendCollection = pSendCollection;
        _sendControlCollection = pSendControlCollection;
        _receiveCursor = pRceieveCursor;
        _receiveControlCollection = pReceiveControlCollection;
        _clientSequenceNum = pClientSequenceNumber;
        _serverSequenceNum = pServerSequenceNumber;
    }

    public OutputStream getOutputStream() {
        if (_sendCollection == null) {
            throw new IllegalStateException("Connection is not open to server");
        }

        return new OutputStream() {
            private int MAX_PAYLOAD_SIZE = 64 * 1024;
            private int MAX_WINDOW_SIZE = 10;
            private boolean closed;

            @Override
            public void write(int b) throws IOException {
                write(new byte[]{(byte) b}, 0, 1);
            }

            @Override
            public void write (byte[] b, int off, int len) throws IOException {
                if (closed) {
                    throw new IOException("Socket closed");
                }

                int bytesRemaining = len;

                while(bytesRemaining > 0) {
                    Document control = _sendControlCollection.find().first();
                    if (_clientSequenceNum - control.getLong("seqNum") > MAX_WINDOW_SIZE) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Write was interrupted", e);
                        }
                        continue;
                    }

                    int bytesToCopy = Math.min(bytesRemaining, MAX_PAYLOAD_SIZE);

                    byte[] copy = new byte[bytesToCopy];
                    System.arraycopy(b, off, copy, 0, bytesToCopy);
                    MongoSocketUtils.writeMessage(_sendCollection,
                            MongoSocketUtils.MessageType.AppData,
                            new Document("bytes", copy),
                            _clientSequenceNum++);

                    bytesRemaining -= bytesToCopy;
                }
            }

            @Override
            public void close() throws IOException {
                MongoSocketUtils.writeMessage(_sendCollection,
                    MongoSocketUtils.MessageType.Close,
                    null,
                    _clientSequenceNum++);
                closed = true;
            }

        };
    }

    public InputStream getInputStream() {
        if (_receiveCursor == null) {
            throw new IllegalStateException("Connection is not open to server");
        }

        return new InputStream() {
            private byte[] _currentPayload;
            private int _currentPayloadPos;
            private boolean closed;

            @Override
            public int read() throws IOException {
                if (_currentPayload.length <= _currentPayloadPos) {
                    fillPayload();
                }
                return _currentPayload[_currentPayloadPos++];
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (b == null) {
                    throw new NullPointerException();
                } else if (off < 0 || len < 0 || len > b.length - off) {
                    throw new IndexOutOfBoundsException();
                } else if (len == 0) {
                    return 0;
                }

                if (_currentPayload == null || _currentPayload.length <= _currentPayloadPos) {
                    fillPayload();
                }

                // close received
                if (_currentPayload == null) {
                    return 0;
                }

                int bytesToCopy = Math.min(len, _currentPayload.length - _currentPayloadPos);
                System.arraycopy(_currentPayload, _currentPayloadPos, b, off, bytesToCopy);
                _currentPayloadPos += bytesToCopy;
                return bytesToCopy;
            }

            private void fillPayload() throws IOException {
                Document d = _receiveCursor.next();
                if (d.containsKey("type") == false || d.getString("type").equals(MongoSocketUtils.MessageType.Close.name())) {
                    _currentPayload = null;
                    _currentPayloadPos = 0;
                    return;
                }

                if (d.containsKey("type") == false || d.getString("type").equals(MongoSocketUtils.MessageType.AppData.name()) == false) {
                    throw new IOException("Unexpected data on socket");
                }

                if (d.containsKey("seqNum") == false || d.getLong("seqNum") != _serverSequenceNum) {
                    throw new IOException(
                            "Failed to handshake to server, did not receive expected sequence number");
                }
                _serverSequenceNum++;

                _currentPayloadPos = 0;
                _currentPayload = ((Binary)((Document)d.get("payload")).get("bytes")).getData();

                _receiveControlCollection.findOneAndUpdate(
                        new Document(),
                        new Document("$set", new Document("seqNum", d.getLong("seqNum"))));
            }
        };
    }
}
