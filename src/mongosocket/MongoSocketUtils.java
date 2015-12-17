package mongosocket;

import com.mongodb.CursorType;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

import java.util.UUID;

class MongoSocketUtils {

    public enum MessageType {
        ClientHello,
        ServerHello,
        ClientPing,
        ServerPong,
        AppData,
        Close
    }

    public static void writeMessage(MongoCollection<Document> pCollection, MessageType pMessageType, Document pPayload, long pSequenceNumber) {
        Document sendDoc = new Document();
        sendDoc.put("seqNum", pSequenceNumber);
        sendDoc.put("type", pMessageType.name());
        sendDoc.put("payload", pPayload);

        pCollection.insertOne(sendDoc);
    }

    public static MongoCursor<Document> createTailingCursor(MongoCollection<Document> pCollection) {
        return pCollection.find()
            .cursorType(CursorType.TailableAwait)
            .iterator();
    }

    public static MongoCursor<Document> createTailingCursorAndCollection(MongoClient pClient, MongoNamespace pNamespace, long pCollectionSize) {
        pClient.getDatabase(pNamespace.getDatabaseName())
            .getCollection(pNamespace.getCollectionName()).drop();

        pClient.getDatabase(pNamespace.getDatabaseName())
            .createCollection(pNamespace.getCollectionName(),
                new CreateCollectionOptions().capped(true).sizeInBytes(pCollectionSize));

        pClient.getDatabase(pNamespace.getDatabaseName())
            .getCollection(pNamespace.getCollectionName())
            .insertOne(new Document("Start", 1));

        MongoCursor<Document> cursor = createTailingCursor(
            pClient.getDatabase(pNamespace.getDatabaseName())
                .getCollection(pNamespace.getCollectionName()));

        cursor.next();
        return cursor;
    }

    public static Document getDocFromTailingCursor(MongoCursor<Document> pCursor, long pTimeout) {
        long timeout = System.currentTimeMillis() + (pTimeout * 1000L);
        while (System.currentTimeMillis() < timeout) {
            Document d = pCursor.tryNext();
            if (d == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                continue;
            }

            return d;
        }

        return null;
    }

    public static String newClientId() {
        return UUID.randomUUID().toString();
    }
}
