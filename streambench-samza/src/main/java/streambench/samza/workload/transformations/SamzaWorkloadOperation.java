package streambench.samza.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.WorkloadOperation;

import java.util.ArrayList;
import java.util.List;

public abstract class SamzaWorkloadOperation extends WorkloadOperation<MessageStream<KV<String, String>>> {

    protected static final Logger logger = LoggerFactory.getLogger(SamzaWorkloadOperation.class);

    protected SamzaWorkloadOperation(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    public static ArrayList<MessageStream<KV<String, String>>> apply(
            String name,
            WorkloadTransformation transformation,
            List<MessageStream<KV<String, String>>> srcStreams) {

        switch (transformation.getOperator()) {
            /* STATELESS OPS */
            case "filter": return new SamzaFilterOp(name, transformation).apply(srcStreams);
            case "split": return new SamzaSplitOp(name, transformation).apply(srcStreams);
            case "modify": return new SamzaModifyOp(name, transformation).apply(srcStreams);
            case "merge": return new SamzaMergeOp(name, transformation).apply(srcStreams);

            /* STATEFUL OPS */
            case "join": return new SamzaJoinOp(name, transformation).apply(srcStreams);
            case "window": return new SamzaWindowOp(name, transformation).apply(srcStreams);
        }

        logger.error("Unknown operator: " + transformation.getOperator());
        throw new StreamBenchException("Unknown operator: " + transformation.getOperator());
    }
}
