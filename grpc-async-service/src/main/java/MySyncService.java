import de.niklaspeter.Confirmation;
import de.niklaspeter.Event;
import de.niklaspeter.MyServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MySyncService extends MyServiceGrpc.MyServiceImplBase {
    private final Semaphore lock = new Semaphore(1);

    @SneakyThrows
    @Override
    public StreamObserver<Event> myServiceMethodA(StreamObserver<Confirmation> responseObserver) {
        log.info(responseObserver + ": Acquiring lock ...");
        if (!lock.tryAcquire(10, TimeUnit.SECONDS)) {
            log.warn(responseObserver + ": Lock acquire timeout exceeded");
            return new StreamObserver<>() {
                @Override
                public void onNext(Event event) {

                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }

        log.info(responseObserver + ": Acquired lock.");

        return new StreamObserver<>() {
            private final List<Event> events = new ArrayList<>();

            @Override
            public void onNext(Event event) {
                log.info(responseObserver + ": Received event.");

                var preprocessedEvent = preprocess(event);
                events.add(preprocessedEvent);
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn(responseObserver + ": Received error.");

                lock.release();
            }

            @Override
            public void onCompleted() {
                log.info(responseObserver + ": Received complete.");

                store(events.toArray(Event[]::new)).handle((unused, throwable) -> {
                    log.info(responseObserver + ": Store completed.");

                    responseObserver.onNext(Confirmation.newBuilder().build());
                    responseObserver.onCompleted();

                    lock.release();

                    return null;
                });
            }

            private Event preprocess(Event event) {
                // The preprocessing already requires the lock.
                return event;
            }
        };
    }

    @SneakyThrows
    @Override
    public void myServiceMethodB(Event event, StreamObserver<Confirmation> responseObserver) {
        if (!lock.tryAcquire(5, TimeUnit.SECONDS))
            throw new TimeoutException("The lock acquire timeout exceeded.");

        // Requires exclusive access to a shared resource and uses async I/O.
        store(event).handle((unused, throwable) -> {
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();

            lock.release();

            return null;
        });
    }

    // This code is actually in a library, which can not be changed.
    @SneakyThrows
    private CompletableFuture<Void> store(Event... events) {
        var fileName = File.createTempFile("grpc-async", "").toPath();
        var fileChannel = AsynchronousFileChannel.open(
                fileName,
                StandardOpenOption.WRITE
        );

        var serializedEvents = serializeEvents(events);

        var completableFuture = new CompletableFuture<Void>();
        fileChannel.write(serializedEvents, 0, null, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, Object attachment) {
                completableFuture.complete(null);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                completableFuture.completeExceptionally(exc);
            }
        });

        return completableFuture.thenApply(unused -> {
            try {
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    private ByteBuffer serializeEvents(Event[] events) {
        return ByteBuffer.wrap(new byte[]{42});
    }

    @Override
    public void otherServiceMethod(Event request, StreamObserver<Confirmation> responseObserver) {
        // Do something independent from the other service methods.
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }
}