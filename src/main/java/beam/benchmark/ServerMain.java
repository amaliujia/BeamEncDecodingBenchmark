package beam.benchmark;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ServerMain {
    public static void main(String[] args) throws Exception {
        DataTransferServer server = new DataTransferServer(8980);
        server.start();
        server.blockUntilShutdown();
    }
}
