package beam.benchmark;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.apache.beam.benchmark.proto.grpc.ElementTransferServiceGrpc.ElementTransferServiceImplBase;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServicePayload;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServiceReply;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

public class DataTransferServer extends ElementTransferServiceImplBase {
    private static final Logger logger = Logger.getLogger(DataTransferServer.class.getName());


    // 4 threads
    private Executor executor = Executors.newFixedThreadPool(4);
    private AtomicInteger count = new AtomicInteger(0);
    private AtomicInteger success = new AtomicInteger(0);
    private AtomicInteger fail = new AtomicInteger(0);

    public DataTransferServer() {
    }

    @Override
    public StreamObserver<ServicePayload> send(StreamObserver<ServiceReply> responseObserver) {
        return new StreamObserver<ServicePayload>() {
            @Override
            public void onNext(ServicePayload payload) {
                count.getAndIncrement();
                logger.info("Receiving a bundle, current count:" + count);
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
                                    fail.getAndIncrement();
                                }
                            }
                            success.getAndIncrement();
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
                logger.info("completing...");
                int acc = 0;
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    int c = count.get();
                    int s = success.get() + fail.get();
                    logger.info("success + fail=" + s);
                    for (int i = 0; i < s - acc; i++) {
                        responseObserver.onNext(ServiceReply.newBuilder().build());
                    }
                    acc = s;
                    if (s == c) {
                        responseObserver.onCompleted();
                        break;
                    }
                }
            }
        };
    }
}
