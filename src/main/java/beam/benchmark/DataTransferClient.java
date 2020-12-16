package beam.benchmark;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.beam.benchmark.proto.grpc.ElementTransferServiceGrpc.ElementTransferServiceStub;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServicePayload;
import org.apache.beam.benchmark.proto.grpc.GrpcProtos.ServiceReply;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow.Coder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.joda.time.Instant;

public class DataTransferClient {
    private static final Logger logger = Logger.getLogger(DataTransferClient.class.getName());
    private final ElementTransferServiceStub asyncStub;
    private int count;
    private final int numBundle;
    private final int numMessagePerBundle;
    private final boolean windowed;
    private final int dataSize;
    private long startTime = 0;

    public DataTransferClient(Channel channel, int a, int b, int dataSize, boolean windowed) {
        asyncStub = org.apache.beam.benchmark.proto.grpc.ElementTransferServiceGrpc.newStub(channel);
        this.numBundle = a;
        this.numMessagePerBundle = b;
        this.windowed = windowed;
        this.dataSize = dataSize;
    }

    public void close() {

    }

    public void sendData() {
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<ServiceReply> replyStreamObserver =
            new StreamObserver<ServiceReply>() {
                @Override
                public void onNext(ServiceReply reply) {
                    count++;
                }

                @Override
                public void onError(Throwable t) {
                    finishLatch.countDown();
                    logger.info(t.toString());
                }

                @Override
                public void onCompleted() {
                    // should end benchmark here
                    logger.info("Count: " + count + ", bundle: " + numBundle + ", message:" + numMessagePerBundle );
                    logger.info("time: " + (System.currentTimeMillis() - startTime));
                    finishLatch.countDown();
                }
             };

        StreamObserver<ServicePayload> requestObserver = asyncStub.send(replyStreamObserver);
        List<ByteString> data = new ArrayList<>();
        byte[] fourKBytes = new byte[dataSize];
        java.util.Random random = new java.util.Random();
        random.nextBytes(fourKBytes);

        if (windowed) {
            WindowedValue<byte[]> value = createWindowedValue(fourKBytes);
            FullWindowedValueCoder<byte[]> coder =
                    FullWindowedValueCoder.of(ByteArrayCoder.of(), Coder.INSTANCE);
            for (int i = 0 ; i < numMessagePerBundle; i++) {
                try {
                    ByteString.Output output = ByteString.newOutput();
                    coder.encode(value, output);
                    data.add(output.toByteString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            ByteArrayCoder coder = ByteArrayCoder.of();
            try {
                ByteString.Output output = ByteString.newOutput();
                coder.encode(fourKBytes, output);
                data.add(output.toByteString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String coder = windowed ? "window" : "nonwindow";
        ServicePayload payload =
                ServicePayload
                    .newBuilder()
                    .setCoder(coder)
                    .addAllData(data)
                    .build();
        startTime = System.currentTimeMillis();
        for (int i = 0; i < numBundle; i++) {
            requestObserver.onNext(payload);
        }
        requestObserver.onCompleted();
        // Receiving happens asynchronously
        try {
            if (!finishLatch.await(5, TimeUnit.MINUTES)) {
                logger.warning("sendData can not finish within 5 minutes");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private WindowedValue<byte[]> createWindowedValue(byte[] fourKBytes) {
         WindowedValue<byte[]> value = WindowedValue.<byte[]>of(
                fourKBytes,
                Instant.now(),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.ON_TIME));
         return value;
    }

    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:8980";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        DataTransferClient client = new DataTransferClient(channel, 1000, 1000, 4096, false);
        client.sendData();
        channel.shutdownNow();
    }
}
