package streambench;

public class StreamBenchException extends RuntimeException {

    public StreamBenchException() {
        super();
    }

    public StreamBenchException(String message) {
        super(message);
    }

    public StreamBenchException(Throwable throwable) {
        super(throwable);
    }

    public StreamBenchException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
