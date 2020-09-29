import de.niklaspeter.Confirmation;
import de.niklaspeter.Event;
import de.niklaspeter.MyServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.common.concurrent.AsyncSemaphore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MyAsyncService extends MyServiceGrpc.MyServiceImplBase {
    private final AsyncSemaphore lock = new AsyncSemaphore(1, Optional.empty());

    @SneakyThrows
    @Override
    public StreamObserver<Event> myServiceMethodA(StreamObserver<Confirmation> responseObserver) {
        log.info(responseObserver + ": Acquiring lock ...");
        var lockFuture = lock.acquire();


        return new StreamObserver<>() {
            private final List<Event> events = new ArrayList<>();

            @Override
            public void onNext(Event event) {
                log.info(responseObserver + ": Received event.");

                events.add(event);
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn(responseObserver + ": Received error.");
//                lockFuture.cancel(true);
//                lockFuture.thenAccept(Permit::release);
            }

            @Override
            public void onCompleted() {
                log.info(responseObserver + ": Received complete; is lock done: " + lockFuture.isDone());

                lockFuture.orTimeout(10, TimeUnit.SECONDS)
                          .thenAccept(permit -> {
                              log.info(responseObserver + ": Acquired lock.");

                              var preprocessedEvents = events.stream().map(this::preprocess).toArray(Event[]::new);
                              store(preprocessedEvents).handle((unused, throwable) -> {
                                  log.info(responseObserver + ": Store completed.");

                                  responseObserver.onNext(Confirmation.newBuilder().build());
                                  responseObserver.onCompleted();

                                  permit.release();

                                  return null;
                              });
                          })
                          .exceptionally(throwable -> {
                              log.warn("Lock acquire failed: " + throwable);
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
        responseObserver.onNext(Confirmation.newBuilder().build());
        responseObserver.onCompleted();
//        if (!lock.tryAcquire(5, TimeUnit.SECONDS))
//            throw new TimeoutException("The lock acquire timeout exceeded.");
//
//        // Requires exclusive access to a shared resource and uses async I/O.
//        store(event).handle((unused, throwable) -> {
//            responseObserver.onNext(Confirmation.newBuilder().build());
//            responseObserver.onCompleted();
//
//            lock.release();
//
//            return null;
//        });
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