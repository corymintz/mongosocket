package mongosocket;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class MongoSocketServer {

    private MongoClient _client;
    private MongoCollection<Document> _serverConnectCollection;
    private MongoNamespace _namespace;
    private MongoSocketServerConnectCallback _callback;
    private long _readWriteTimeout;

    public MongoSocketServer(MongoNamespace pNamespace) {
        this(new MongoClient(), pNamespace);
    }

    public MongoSocketServer(MongoClient pClient, MongoNamespace pNamespace) {
        _client = pClient;
        _namespace = pNamespace;
        _serverConnectCollection = _client
            .getDatabase(pNamespace.getDatabaseName())
            .getCollection(pNamespace.getCollectionName());
        _readWriteTimeout = 60;
    }

    public void setCallback(MongoSocketServerConnectCallback pCallback) {
        _callback = pCallback;
    }

    public void setReadWriteTimeout(long pReadWriteTimeout) { _readWriteTimeout = pReadWriteTimeout; }

    public void start() {
        Runnable task = () -> {
            MongoCursor<Document> cursor = MongoSocketUtils.createTailingCursorAndCollection(_client, _namespace, 1024 * 1024);

            while (true) {
                Document d = MongoSocketUtils.getDocFromTailingCursor(cursor, Integer.MAX_VALUE);
                if (d == null) {
                    continue;
                }

                if (d.containsKey("type") == false ||
                    d.getString("type").equals(MongoSocketUtils.MessageType.ClientHello.name()) == false) {
                    continue;
                }

                Document payload = (Document) d.get("payload");
                if (payload.containsKey("clientId") == false) {
                    continue;
                }

                createClientConnection(payload.getString("clientId"), 5000);
            }

        };

        new Thread(task).start();
    }

    private void createClientConnection(String pClientId, long pHandshakeTimeout) {
        Runnable task = () -> {
            int clientSequenceNum = 1;
            int serverSequenceNum = 0;

            MongoNamespace sendNamespace = new MongoNamespace(
                _namespace.getDatabaseName(),
                _namespace.getCollectionName() + MongoSocketUtils.newClientId());
            MongoNamespace sendControlNamespace = new MongoNamespace(
                sendNamespace.getDatabaseName(),
                sendNamespace.getCollectionName() + "_control");
            MongoNamespace receiveNamespace = new MongoNamespace(
                _namespace.getDatabaseName(),
                _namespace.getCollectionName() + MongoSocketUtils.newClientId());
            MongoNamespace receiveControl = new MongoNamespace(
                receiveNamespace.getDatabaseName(),
                receiveNamespace.getCollectionName() + "_control");

            MongoCollection<Document> sendControlCollection = _client
                    .getDatabase(sendControlNamespace.getDatabaseName())
                    .getCollection(sendControlNamespace.getCollectionName());
            MongoCollection<Document> receiveCollection = _client
                    .getDatabase(receiveNamespace.getDatabaseName())
                    .getCollection(receiveNamespace.getCollectionName());
            MongoCollection<Document> receiveControlCollection = _client
                    .getDatabase(receiveControl.getDatabaseName())
                    .getCollection(receiveControl.getCollectionName());

            Document serverHelloPayload = new Document();
            serverHelloPayload.put("clientId", pClientId);
            serverHelloPayload.put("sendNamespace", sendNamespace.getFullName());
            serverHelloPayload.put("sendControl", sendControlNamespace.getFullName());
            serverHelloPayload.put("receiveNamespace", receiveNamespace.getFullName());
            serverHelloPayload.put("receiveControl", receiveControl.getFullName());

            receiveControlCollection.insertOne(new Document("seqNum", 1L));
            sendControlCollection.insertOne(new Document("seqNum", 1L));

            // start listening on the send namespace
            MongoCursor<Document> sendCursor = MongoSocketUtils.createTailingCursorAndCollection(
                    _client,
                    sendNamespace,
                    MongoSocket.MAX_PAYLOAD_SIZE * (MongoSocket.MAX_WINDOW_SIZE + 5));

            // send the server hello
            try {
                MongoSocketUtils.writeMessage(_serverConnectCollection,
                        MongoSocketUtils.MessageType.ServerHello,
                        serverHelloPayload,
                        serverSequenceNum++);
            } catch (MongoException e) {
                System.out.println("Failed to send `Hello` to MongoDB server: " + e.getMessage());
            }

            // wait for a ping
            long timeout = System.currentTimeMillis() + (pHandshakeTimeout * 1000L);
            Document d = MongoSocketUtils.getDocFromTailingCursor(
                    sendCursor,
                    timeout - System.currentTimeMillis());
            if (d == null) {
                System.out.println("Timeout waiting for connection");
                return;
            }

            if (d.containsKey("type") == false ||
                    d.getString("type").equals(MongoSocketUtils.MessageType.ClientPing.name()) == false) {
                System.out.println("Failed to handshake to server, did not receive expected `Pong`");
                return;
            }

            Document payload = (Document) d.get("payload");
            if (payload.containsKey("clientId") == false || payload.getString("clientId").equals(pClientId) == false) {
                System.out.println("Failed to handshake to server, did not receive `clientId`");
                return;
            }

            if (d.containsKey("seqNum") == false || d.getLong("seqNum") != clientSequenceNum) {
                System.out.println("Failed to handshake to server, did not receive expected sequence number");
                return;
            }
            clientSequenceNum++;

            //write a pong
            try {
                MongoSocketUtils.writeMessage(receiveCollection,
                        MongoSocketUtils.MessageType.ServerPong,
                        new Document("clientId", pClientId),
                        serverSequenceNum++);
            } catch (MongoException e) {
                System.out.println("Failed to send `Ping` to MongoDB server: " + e.getMessage());
            }

            MongoSocket s = new MongoSocket(_readWriteTimeout,
                receiveCollection,
                receiveControlCollection,
                sendCursor,
                sendControlCollection,
                serverSequenceNum,
                clientSequenceNum);
            if (_callback != null) {
                _callback.connectionEstablished(s);
            }
        };

        new Thread(task).start();
    }

    public interface MongoSocketServerConnectCallback {
        void connectionEstablished(MongoSocket pMongoSocket);
    }
}
