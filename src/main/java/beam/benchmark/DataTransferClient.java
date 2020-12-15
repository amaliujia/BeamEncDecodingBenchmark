package beam.benchmark;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    public DataTransferClient(Channel channel) {
        asyncStub = org.apache.beam.benchmark.proto.grpc.ElementTransferServiceGrpc.newStub(channel);
    }

    public void sendData(int bundle, int numMessagePerBundle) {
        StreamObserver<ServiceReply> replyStreamObserver =
            new StreamObserver<ServiceReply>() {
                @Override
                public void onNext(ServiceReply reply) {

                }

                @Override
                public void onError(Throwable t) {
                    logger.info(t.toString());
                }

                @Override
                public void onCompleted() {
                }
             };

        StreamObserver<ServicePayload> requestObserver = asyncStub.send(replyStreamObserver);
        List<ByteString> data = new ArrayList<>();
        byte[] fourKBytes = new byte[4096];
        java.util.Random random = new java.util.Random();
        random.nextBytes(fourKBytes);
        FullWindowedValueCoder<byte[]> coder =
                FullWindowedValueCoder.of(ByteArrayCoder.of(), Coder.INSTANCE);
        WindowedValue<byte[]> value = WindowedValue.<byte[]>of(
                fourKBytes,
                Instant.now(),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.ON_TIME));

        for (int i = 0 ; i < numMessagePerBundle; i++) {
            try {
                ByteString.Output output = ByteString.newOutput();
                coder.encode(value, output);
                data.add(output.toByteString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ServicePayload payload =
                ServicePayload
                    .newBuilder()
                    .setCoder("window")
                    .addAllData(data)
                    .build();
        for (int i = 0; i < bundle; i++) {
            requestObserver.onNext(payload);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:8980";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        DataTransferClient client = new DataTransferClient(channel);
    }
}
