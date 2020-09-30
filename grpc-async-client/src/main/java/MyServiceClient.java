import de.niklaspeter.Confirmation;
import de.niklaspeter.Event;
import de.niklaspeter.MyServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MyServiceClient {
    private final MyServiceGrpc.MyServiceStub stub;
    private final ManagedChannel channel;

    public MyServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public MyServiceClient(ManagedChannelBuilder<?> managedChannelBuilder) {
        channel = managedChannelBuilder.build();
        stub = MyServiceGrpc.newStub(channel);
    }

    @SneakyThrows
    public void myServiceMethodA() {
        log.info("myServiceMethodA(): started.");
        var writeConnection = stub.myServiceMethodA(new LoggingStreamObserver());
        log.info("myServiceMethodA(): received write connection.");

        writeConnection.onNext(createEvent());
        writeConnection.onCompleted();
    }

    public void myServiceMethodB() {
        stub.myServiceMethodB(createEvent(), new LoggingStreamObserver());
    }

    private Event createEvent() {
        return Event.newBuilder().setData(UUID.randomUUID().toString()).build();
    }

    @SneakyThrows
    public void shutdown() {
        channel.awaitTermination(5, TimeUnit.MINUTES);
    }

    private static class LoggingStreamObserver implements StreamObserver<Confirmation> {
        @Override
        public void onNext(Confirmation confirmation) {
            log.info("Received confirmation.");
        }

        @Override
        public void onError(Throwable throwable) {
            log.warn("Received error: " + throwable.getMessage());
        }

        @Override
        public void onCompleted() {
            log.info("Received completed.");
        }
    }
}
