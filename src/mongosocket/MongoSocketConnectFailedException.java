package mongosocket;

public class MongoSocketConnectFailedException extends Exception {
    public MongoSocketConnectFailedException(String pMessage) {
        super(pMessage);
    }

    public MongoSocketConnectFailedException(String pMessage, Exception pCausedBy) {
        super(pMessage, pCausedBy);
    }
}
