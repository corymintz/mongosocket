package mongosocket;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import mongosocket.MongoSocketUtils.MessageType;

public class MongoSocketClient {

    private MongoClient _client;
    private MongoCollection<Document> _serverConnectCollection;

    public MongoSocketClient(MongoClient pClient, MongoNamespace pNamespace) {
        _client = pClient;
        _serverConnectCollection = _client
                .getDatabase(pNamespace.getDatabaseName())
                .getCollection(pNamespace.getCollectionName());
    }

    public MongoSocket connect(long pConnectTimeout) throws MongoSocketConnectFailedException {
        long clientSequenceNum = 0;
        long serverSequenceNum = 0;
        MongoCollection<Document> sendCollection = null;
        MongoCollection<Document> sendControlCollection = null;
        MongoCollection<Document> receiveCollection = null;
        MongoCollection<Document> receiveControlCollection = null;

        // send hello on server namespace where hopefully a server is listening
        String clientId = MongoSocketUtils.newClientId();
        try {
            MongoSocketUtils.writeMessage(_serverConnectCollection,
                MessageType.ClientHello,
                new Document("clientId", clientId),
                clientSequenceNum++);
        } catch (MongoException e) {
            throw new MongoSocketConnectFailedException("Failed to send `Hello` to MongoDB server", e);
        }

        // wait for the server to response to the clientId
        long timeout = System.currentTimeMillis() + (pConnectTimeout * 1000L);
        MongoCursor<Document> serverHelloCursor = MongoSocketUtils.createTailingCursor(_serverConnectCollection);
        try {
            while (System.currentTimeMillis() < timeout) {
                Document d = MongoSocketUtils.getDocFromTailingCursor(
                    serverHelloCursor,
                    System.currentTimeMillis() - timeout);
                if (d == null) {
                    throw new MongoSocketConnectFailedException("Timeout waiting for connection");
                }

                if (d.containsKey("type") == false || d.getString("type").equals(MessageType.ServerHello.name()) == false) {
                    continue;
                }

                Document payload = (Document) d.get("payload");
                if (payload.containsKey("clientId") == false) {
                    continue;
                }

                if (payload.getString("clientId").equals(clientId) == false) {
                    continue;
                }

                if (d.containsKey("seqNum") == false || d.getLong("seqNum") != serverSequenceNum) {
                    throw new MongoSocketConnectFailedException(
                            "Failed to handshake to server, did not receive expected sequence number");
                }
                serverSequenceNum++;

                MongoNamespace sendNamespace = new MongoNamespace(payload.getString("sendNamespace"));
                sendCollection = _client
                        .getDatabase(sendNamespace.getDatabaseName())
                        .getCollection(sendNamespace.getCollectionName());
                MongoNamespace sendControl = new MongoNamespace(payload.getString("sendControl"));
                sendControlCollection = _client
                        .getDatabase(sendControl.getDatabaseName())
                        .getCollection(sendControl.getCollectionName());

                MongoNamespace receiveNamespace = new MongoNamespace(payload.getString("receiveNamespace"));
                receiveCollection = _client
                        .getDatabase(receiveNamespace.getDatabaseName())
                        .getCollection(receiveNamespace.getCollectionName());
                MongoNamespace receiveControl = new MongoNamespace(payload.getString("receiveControl"));
                receiveControlCollection = _client
                        .getDatabase(receiveControl.getDatabaseName())
                        .getCollection(receiveControl.getCollectionName());
            }
        } finally {
            serverHelloCursor.close();
        }

        // timeout
        if (sendCollection == null) {
            throw new MongoSocketConnectFailedException("Failed to handshake to server, timeout reached");
        }

        // start listening on the receive namespace
        MongoCursor<Document> receiveCursor = MongoSocketUtils.createTailingCursor(receiveCollection);

        // send a ping on the send namespace
        try {
            MongoSocketUtils.writeMessage(sendCollection,
                    MessageType.ClientPing,
                    new Document("clientId", clientId),
                    clientSequenceNum++);
        } catch (MongoException e) {
            throw new MongoSocketConnectFailedException("Failed to send `Ping` to MongoDB server", e);
        }

        // wait for a pong on the receive namespace
        while (System.currentTimeMillis() < timeout) {
            Document d = MongoSocketUtils.getDocFromTailingCursor(
                receiveCursor,
                System.currentTimeMillis() - timeout);
            if (d == null) {
                throw new MongoSocketConnectFailedException("Timeout waiting for connection");
            }

            if (d.containsKey("type") == false || d.getString("type").equals(MessageType.ServerPong.name()) == false) {
                throw new MongoSocketConnectFailedException(
                        "Failed to handshake to server, did not receive expected `Pong`");
            }

            Document payload = (Document) d.get("payload");
            if (payload.containsKey("clientId") == false || payload.getString("clientId").equals(clientId) == false) {
                throw new MongoSocketConnectFailedException(
                        "Failed to handshake to server, did not receive `clientId`");
            }

            if (d.containsKey("seqNum") == false || d.getLong("seqNum") != serverSequenceNum) {
                throw new MongoSocketConnectFailedException(
                        "Failed to handshake to server, did not receive expected sequence number");
            }
            serverSequenceNum++;
        }

        return new MongoSocket(sendCollection,
                sendControlCollection,
                receiveCursor,
                receiveControlCollection,
                clientSequenceNum,
                serverSequenceNum);
    }

}
