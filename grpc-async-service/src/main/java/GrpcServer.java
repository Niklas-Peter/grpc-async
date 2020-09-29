import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Slf4j
public class GrpcServer {
    private final Server server;
    private final int port;

    public GrpcServer(int port) {
        this.port = port;

        server = ServerBuilder
                .forPort(port)
                .executor(Executors.newSingleThreadExecutor())
                .addService(new MyAsyncService())
                .build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        log.info(String.format("GRPC server started on port %d.", port));
        server.awaitTermination();
    }

    public void stop() {
        server.shutdown();
    }
}

