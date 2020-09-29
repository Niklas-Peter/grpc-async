import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GrpcAsyncService {
    public static void main(String... args) {
        var loggers = new String[]{"io.netty", "io.grpc"};
        for (var logger : loggers) {
            var root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(logger);
            root.setLevel(Level.INFO);
        }

        final var port = 50123;
        new Thread(() -> {
            try {
                new GrpcServer(port).start();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
