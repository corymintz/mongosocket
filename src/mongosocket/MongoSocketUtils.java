package mongosocket;

import com.mongodb.CursorType;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
        sendDoc.put("type", pMessageType);
        sendDoc.put("payload", pPayload);

        pCollection.insertOne(sendDoc);
    }

    public static MongoCursor<Document> createTailingCursor(MongoCollection<Document> pCollection) {
        return pCollection.find()
                .cursorType(CursorType.TailableAwait)
                .sort(new Document("$natrual", 1))
                .iterator();
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
