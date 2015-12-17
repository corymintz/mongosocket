package mongosocket.file;

import java.io.*;
import java.nio.file.Files;

import mongosocket.MongoSocket;
import mongosocket.MongoSocketServer;
import mongosocket.MongoSocketServer.MongoSocketServerConnectCallback;

public class MongoFileServer {

    public MongoFileServer(MongoSocketServer pServer, File pDirectory) {
        pServer.setCallback(new Callback(pDirectory));
    }

    private class Callback implements  MongoSocketServerConnectCallback {
        private File _directory;
        public Callback(File pDirectory) {
            _directory = pDirectory;
        }

        public void connectionEstablished(MongoSocket pMongoSocket) {
            OutputStream out = pMongoSocket.getOutputStream();
            String relPath;

            try {
                relPath = readRelativePath(pMongoSocket.getInputStream());

                File file = new File(_directory, relPath);
                if (file.exists() == false) {
                    return; // need to build this into the protocol
                }

                sendFile(file, out);
            } catch (IOException e) {
                // log something
            } finally {
                try {
                    out.close();
                } catch (IOException e) {}
            }
        }

        private String readRelativePath(InputStream s) throws IOException {
            StringBuilder relPath = new StringBuilder();
            byte[] buffer = new byte[1024];

            while (true) {
                int read = s.read(buffer);
                if (read == 0) {
                    break;
                }

                relPath.append(new String(buffer, 0, read));
            }

            return relPath.toString();
        }

        private void sendFile(File f, OutputStream s) throws IOException {
            Files.copy(f.toPath(), s);
        }
    }


}
