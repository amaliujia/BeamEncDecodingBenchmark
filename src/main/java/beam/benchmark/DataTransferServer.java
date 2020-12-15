package beam.benchmark;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.beam.benchmark.proto.grpc.ElementTransferServiceGrpc.ElementTransferServiceImplBase;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServicePayload;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServiceReply;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

public class DataTransferServer extends ElementTransferServiceImplBase {
    private static final Logger logger = Logger.getLogger(DataTransferServer.class.getName());

//    public final ConcurrentLinkedQueue<org.apache.beam.benchmark.proto.grpc.GrpcProtos.Payload> queue;

    private final Server server;
    // 4 threads
    private Executor executor = Executors.newFixedThreadPool(4);
    public DataTransferServer(int port) {
//        this.queue = q;
        server = ServerBuilder.forPort(port).build();
    }

    @Override
    public StreamObserver<ServicePayload> send(StreamObserver<ServiceReply> responseObserver) {
        return new StreamObserver<ServicePayload>() {
            @Override
            public void onNext(ServicePayload payload) {
                executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            Coder coder = null;
                            if (payload.getCoder().equals("window")) {
                                coder = FullWindowedValueCoder.of(
                                        ByteArrayCoder.of(),
                                        GlobalWindow.Coder.INSTANCE
                                );
                            } else {
                                coder = ByteArrayCoder.of();
                            }

                            for (ByteString data : payload.getDataList()) {
                                try {
                                    coder.decode(data.newInput());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            responseObserver.onNext(ServiceReply.newBuilder().build());
                        }
                    }
                );
            }

            @Override
            public void onError(Throwable t) {
                logger.info("send cancelled");
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    DataTransferServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
