package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class WorkloadOperation<T> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadOperation.class);

    // TODO: implement CPU load per operation
    /* load generation reference: https://caffinc.github.io/2016/03/cpu-load-generator/ */
    protected static final long SLEEP_DURATION = 500; // 0.5 second
    protected static final double SLEEP_LOAD = 0.8;

    protected String name;
    protected WorkloadTransformation transformation;

    /* TODO: figure out how to make the function below static and enforce subclasses to implement */
//    public static abstract ArrayList<T> apply(String name, WorkloadTransformation transformation, List<T> srcStreams);

    protected WorkloadOperation(String name, WorkloadTransformation transformation) {
        this.name = name;
        this.transformation = transformation;
    }

    public abstract ArrayList<T> apply(List<T> srcStreams);

    protected Duration parseDuration(String durationString) {
        final Pattern pattern = Pattern.compile("(?<integer>\\d+)(?<unit>(ms|[ms]))");

        Matcher matcher = pattern.matcher(durationString);
        if(!matcher.matches())
            throw new StreamBenchException("Invalid duration parameter: " + durationString);

        Integer durationInt = Integer.valueOf(matcher.group("integer"));
        String unit = matcher.group("unit");
        switch (unit) {
            case "ms": return Duration.ofMillis(durationInt);
            case "s": return Duration.ofSeconds(durationInt);
            case "m": return Duration.ofMinutes(durationInt);
            default: throw new StreamBenchException("Invalid duration unit: " + unit);
        }
    }
}
