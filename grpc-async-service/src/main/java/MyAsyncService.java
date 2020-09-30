import de.niklaspeter.Confirmation;
import de.niklaspeter.Event;
import de.niklaspeter.MyServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.common.concurrent.AsyncSemaphore;
import org.apache.distributedlog.common.util.Permit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MyAsyncService extends MyServiceGrpc.MyServiceImplBase {
    private final AsyncSemaphore lock = new AsyncSemaphore(1, Optional.empty());

    @SneakyThrows
    @Override
    public StreamObserver<Event> myServiceMethodA(StreamObserver<Confirmation> responseObserver) {
        log.info(responseObserver + ": Acquiring lock ...");
        // Already acquire the lock here (instead of in StreamObserver.onComplete())
        // to ensure the lock is acquired in the order,
        // in which the GRPC requests are handled, and not in the order, in which the GRPC stream observers
        // complete.
        var lockFuture = lock.acquire();

        return new StreamObserver<>() {
            private final List<Event> events = new ArrayList<>();

            @Override
            public void onNext(Event event) {
                log.info(responseObserver + ": Received event.");

                events.add(event);
            }

            @Override
            public void onCompleted() {
                log.info(responseObserver + ": Received complete; acquired lock: " + lockFuture.isDone());

                lockFuture.thenAccept(permit -> {
                    log.info(responseObserver + ": Acquired lock.");

                    var preprocessedEvents = events.stream()
                                                   .map(MyAsyncService.this::preprocess)
                                                   .toArray(Event[]::new);
                    var storageLibrary = new StorageLibrary();
                    storageLibrary.store(preprocessedEvents).handle((unused, throwable) -> {
                        log.info(responseObserver + ": Store completed.");

                        responseObserver.onNext(Confirmation.newBuilder().build());
                        responseObserver.onCompleted();

                        permit.release();

                        return null;
                    });
                })
                          .orTimeout(10, TimeUnit.SECONDS)
                          .exceptionally(throwable -> {
                              log.warn("Lock acquire failed: " + throwable);
                              return null;
                          });
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn(responseObserver + ": Received error.");

                lockFuture.cancel(true);
                lockFuture.thenAccept(Permit::release);
            }
        };
    }

    @SneakyThrows
    @Override
    public void myServiceMethodB(Event event, StreamObserver<Confirmation> responseObserver) {
        lock.acquireAndRun(() -> {
            log.info(responseObserver + ": Acquired lock.");

            // Requires exclusive access to a shared resource and uses async I/O.
            var preprocessedEvent = preprocess(event);
            var storageLibrary = new StorageLibrary();
            return storageLibrary.store(preprocessedEvent).handle((unused, throwable) -> {
                responseObserver.onNext(Confirmation.newBuilder().build());
                responseObserver.onCompleted();

                return null;
            });
        })
            .orTimeout(10, TimeUnit.SECONDS)
            .exceptionally(throwable -> {
                log.warn("Lock acquire failed: " + throwable);
                return null;
            });
    }

    private Event preprocess(Event event) {
        // The preprocessing already requires the lock.
        return event;
    }

    @Override
    public void otherServiceMethod(Event request, StreamObserver<Confirmation> responseObserver) {
        // Do something independent from the other service methods.
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
    }
}