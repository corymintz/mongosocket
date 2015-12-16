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

    public MongoSocketServer(MongoClient pClient, MongoNamespace pNamespace) {
        _client = pClient;
        _namespace = pNamespace;
        _serverConnectCollection = _client
            .getDatabase(pNamespace.getDatabaseName())
            .getCollection(pNamespace.getCollectionName());
    }

    public void setCallback(MongoSocketServerConnectCallback pCallback) {
        _callback = pCallback;
    }

    public void start() {
        Runnable task = () -> {
            MongoCursor<Document> cursor = MongoSocketUtils.createTailingCursor(_serverConnectCollection);
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
            int clientSequenceNum = 0;
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

            MongoCollection<Document> sendCollection = _client
                    .getDatabase(sendNamespace.getDatabaseName())
                    .getCollection(sendNamespace.getCollectionName());
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

            // start listening on the send namespace
            MongoCursor<Document> sendCursor = MongoSocketUtils.createTailingCursor(sendCollection);

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
            while (System.currentTimeMillis() < timeout) {
                Document d = MongoSocketUtils.getDocFromTailingCursor(
                    sendCursor,
                    System.currentTimeMillis() - timeout);
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
                }
                clientSequenceNum++;
            }

            //write a pong
            try {
                MongoSocketUtils.writeMessage(receiveCollection,
                        MongoSocketUtils.MessageType.ClientPing,
                        new Document("clientId", pClientId),
                        clientSequenceNum++);
            } catch (MongoException e) {
                System.out.println("Failed to send `Ping` to MongoDB server: " + e.getMessage());
            }

            MongoSocket s = new MongoSocket(receiveCollection,
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
