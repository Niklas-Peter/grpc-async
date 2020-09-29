import ch.qos.logback.classic.Level;
import lombok.SneakyThrows;
import org.slf4j.LoggerFactory;

public class GrpcAsyncClient {
    @SneakyThrows
    public static void main(String... args) {
        var loggers = new String[]{"io.netty", "io.grpc"};
        for (var logger : loggers) {
            var root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(logger);
            root.setLevel(Level.INFO);
        }

        var myServiceClient = new MyServiceClient("localhost", 50123);

        for (var i = 0; i < 5; i++)
            myServiceClient.myServiceMethodA();

        Thread.sleep(5000);

        myServiceClient.shutdown();
    }
}

