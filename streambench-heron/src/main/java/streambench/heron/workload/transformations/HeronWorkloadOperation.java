package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.WorkloadOperation;

import java.util.ArrayList;
import java.util.List;

public abstract class HeronWorkloadOperation extends WorkloadOperation<Streamlet<KeyValue<String, String>>> {

    protected static final Logger logger = LoggerFactory.getLogger(HeronWorkloadOperation.class);

    protected HeronWorkloadOperation(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    public static ArrayList<Streamlet<KeyValue<String, String>>> apply(
            String name,
            WorkloadTransformation transformation,
            List<Streamlet<KeyValue<String, String>>> srcStreams) {

        switch (transformation.getOperator()) {
            /* STATELESS OPS */
            case "filter": return new HeronFilterOp(name, transformation).apply(srcStreams);
            case "split": return new HeronSplitOp(name, transformation).apply(srcStreams);
            case "modify": return new HeronModifyOp(name, transformation).apply(srcStreams);
            case "merge": return new HeronMergeOp(name, transformation).apply(srcStreams);

            /* STATEFUL OPS */
            case "join": return new HeronJoinOp(name, transformation).apply(srcStreams);
            case "window": return new HeronWindowOp(name, transformation).apply(srcStreams);
        }

        logger.error("Unknown operator: " + transformation.getOperator());
        throw new StreamBenchException("Unknown operator: " + transformation.getOperator());
    }
}
