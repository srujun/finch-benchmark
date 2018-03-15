package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadOperation.class);

    // TODO: implement CPU load per operation
    /* load generation reference: https://caffinc.github.io/2016/03/cpu-load-generator/ */
    protected static final long SLEEP_DURATION = 500; // 0.5 second
    protected static final double SLEEP_LOAD = 0.8;

    protected String name;
    protected WorkloadTransformation transformation;

    public static ArrayList<MessageStream<KV<String, String>>> apply(
            String name,
            WorkloadTransformation transformation,
            List<MessageStream<KV<String, String>>> srcStreams) {

        switch (transformation.getOperator()) {
            /* STATELESS OPS */
            case "filter": return new FilterOp(name, transformation).apply(srcStreams);
            case "split": return new SplitOp(name, transformation).apply(srcStreams);
            case "modify": return new ModifyOp(name, transformation).apply(srcStreams);
            case "merge": return new MergeOp(name, transformation).apply(srcStreams);

            /* STATEFUL OPS */
            case "join": return new JoinOp(name, transformation).apply(srcStreams);
            case "window": return new WindowOp(name, transformation).apply(srcStreams);
        }

        logger.error("Unknown operator: " + transformation.getOperator());
        throw new SamzaException("Unknown operator: " + transformation.getOperator());
    }

    WorkloadOperation(String name, WorkloadTransformation transformation) {
        this.name = name;
        this.transformation = transformation;
    }

    public abstract ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams);

    Duration parseDuration(String durationString) {
        final Pattern pattern = Pattern.compile("(?<integer>\\d+)(?<unit>(ms|[ms]))");

        Matcher matcher = pattern.matcher(durationString);
        if(!matcher.matches())
            throw new SamzaException("Invalid duration parameter: " + durationString);

        Integer durationInt = Integer.valueOf(matcher.group("integer"));
        String unit = matcher.group("unit");
        switch (unit) {
            case "ms": return Duration.ofMillis(durationInt);
            case "s": return Duration.ofSeconds(durationInt);
            case "m": return Duration.ofMinutes(durationInt);
            default: throw new SamzaException("Invalid duration unit: " + unit);
        }
    }
}
