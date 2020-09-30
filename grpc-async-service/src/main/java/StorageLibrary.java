import de.niklaspeter.Event;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;

public class StorageLibrary {
    @SneakyThrows
    public CompletableFuture<Void> store(Event... events) {
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

}
