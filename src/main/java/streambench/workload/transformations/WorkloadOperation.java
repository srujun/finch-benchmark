package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;

public abstract class WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadOperation.class);

    protected static final long SLEEP_DURATION = 500; // 0.5 second
    protected static final double SLEEP_LOAD = 0.8;

    protected WorkloadTransformation transformation;

    public static ArrayList<MessageStream<KV<String, String>>> apply(WorkloadTransformation transformation, List<MessageStream<KV<String, String>>> srcStreams) {
        switch (transformation.getOperator()) {
            case "filter": return new FilterOp(transformation).apply(srcStreams);
            case "split": return new SplitOp(transformation).apply(srcStreams);
            case "modify": return new ModifyOp(transformation).apply(srcStreams);
            case "merge": return new MergeOp(transformation).apply(srcStreams);
        }

        logger.error("Unknown operator: " + transformation.getOperator());
        throw new SamzaException("Unknown operator: " + transformation.getOperator());
    }

    public WorkloadOperation(WorkloadTransformation transformation) {
        this.transformation = transformation;
    }

    public abstract ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStream);
}
