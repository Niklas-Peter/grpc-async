import de.niklaspeter.Confirmation;
import de.niklaspeter.Event;
import de.niklaspeter.MyServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class MySyncService extends MyServiceGrpc.MyServiceImplBase {
    private final Semaphore lock = new Semaphore(1);

    @SneakyThrows
    @Override
    public StreamObserver<Event> myServiceMethodA(StreamObserver<Confirmation> responseObserver) {
        log.info(responseObserver + ": Acquiring lock ...");
        if (!lock.tryAcquire(10, TimeUnit.SECONDS)) {
            log.warn(responseObserver + ": Lock acquire timeout exceeded");
            return new NoOpEventStreamObserver(); // Only to prevent exceptions in the log.
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

                var storageLibrary = new StorageLibrary();
                storageLibrary.store(events.toArray(Event[]::new)).handle((unused, throwable) -> {
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
        var storageLibrary = new StorageLibrary();
        storageLibrary.store(event).handle((unused, throwable) -> {
            responseObserver.onNext(Confirmation.newBuilder().build());
            responseObserver.onCompleted();

            lock.release();

            return null;
        });
    }


    @Override
    public void otherServiceMethod(Event request, StreamObserver<Confirmation> responseObserver) {
        // Do something independent from the other service methods.
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static class NoOpEventStreamObserver implements StreamObserver<Event> {
        @Override
        public void onNext(Event event) {

        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
    }
}