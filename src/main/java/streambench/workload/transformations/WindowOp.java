package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;

import java.io.Serializable;
import java.time.Duration;

public abstract class WindowOp<T> extends WorkloadOperation<T> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(WindowOp.class);
    protected static final String PARAM_WINDOW_TYPE = "type";
    protected static final String PARAM_WINDOW_TRIGGER = "trigger";
    protected static final String PARAM_WINDOW_DURATION = "duration";
    protected static final String PARAM_WINDOW_KEYED = "is_keyed";

    protected enum WindowType {
        TUMBLING, SESSION
    }
    // protected enum WindowTrigger {
    //     EARLY, LATE
    // }

    protected WindowType windowType;
    /* TODO: Add support for window triggers */
    // protected WindowTrigger windowTrigger;
    protected Duration windowDuration;
    /* TODO: Only supported keyed windows currently, also support unkeyed */
    // protected boolean isKeyed;

    public WindowOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        String windowTypeString = (String) transformation.getParams().get(PARAM_WINDOW_TYPE);
        switch (windowTypeString) {
            case "tumbling": this.windowType = WindowType.TUMBLING; break;
            case "session": this.windowType = WindowType.SESSION; break;
            default: throw new StreamBenchException("Unknown window type: " + windowTypeString);
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
}
