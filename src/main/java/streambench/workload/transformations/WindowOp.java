package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class WindowOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(WindowOp.class);
    private static final String PARAM_WINDOW_TYPE = "type";
    private static final String PARAM_WINDOW_TRIGGER = "trigger";
    private static final String PARAM_WINDOW_DURATION = "duration";
    private static final String PARAM_WINDOW_KEYED = "is_keyed";

    private enum WindowType {
        TUMBLING, SESSION
    }
    // private enum WindowTrigger {
    //     EARLY, LATE
    // }

    private WindowType windowType;
    /* TODO: Add support for window triggers */
    // private WindowTrigger windowTrigger;
    private Duration windowDuration;
    /* TODO: Only supported keyed windows currently, also support unkeyed */
    // private boolean isKeyed;

    public WindowOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        String windowTypeString = (String) transformation.getParams().get(PARAM_WINDOW_TYPE);
        switch (windowTypeString) {
            case "tumbling": this.windowType = WindowType.TUMBLING; break;
            case "session": this.windowType = WindowType.SESSION; break;
            default: throw new SamzaException("Unknown window type: " + windowTypeString);
        }

        // String windowTriggerString = (String) transformation.getParams().get(PARAM_WINDOW_TRIGGER);
        // switch (windowTriggerString) {
        //     case "early": this.windowTrigger = WindowTrigger.EARLY; break;
        //     case "late": this.windowTrigger = WindowTrigger.LATE; break;
        //     default: throw new SamzaException("Unknown window trigger: " + windowTypeString);
        // }

        this.windowDuration = parseDuration((String) transformation.getParams().get(PARAM_WINDOW_DURATION));
        // this.isKeyed = (Boolean) transformation.getParams().get(PARAM_WINDOW_KEYED);

        logger.info("New window: " + windowTypeString + ", " + windowDuration.toString());
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);
        MessageStream<KV<String, String>> outStream;

        Function<KV<String, String>, String> keyFn = KV::getKey;
        Supplier<String> initialValue = () -> "";
        FoldLeftFunction<KV<String, String>, String> aggregator = (newMsg, oldValue) -> newMsg.getValue();

        switch (this.windowType) {
            case TUMBLING:
                outStream = srcStream
                        .window(
                                Windows.keyedTumblingWindow(
                                        keyFn,
                                        this.windowDuration,
                                        initialValue,
                                        aggregator,
                                        new StringSerde(),
                                        new StringSerde()),
                                this.name)
                        .map(windowPane -> KV.of(windowPane.getKey().getKey(), windowPane.getMessage()));
                break;
            case SESSION:
                outStream = srcStream
                        .window(
                                Windows.keyedSessionWindow(
                                        keyFn,
                                        this.windowDuration,
                                        initialValue,
                                        aggregator,
                                        new StringSerde(),
                                        new StringSerde()),
                                this.name)
                        .map(windowPane -> KV.of(windowPane.getKey().getKey(), windowPane.getMessage()));
                break;
            default: outStream = null; break; // to stop "not initialized" errors
        }

        ArrayList<MessageStream<KV<String, String>>> outStreams = new ArrayList<>();
        outStreams.add(outStream);
        return outStreams;
    }
}
