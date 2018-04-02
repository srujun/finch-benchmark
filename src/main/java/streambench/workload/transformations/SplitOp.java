package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;

public abstract class SplitOp<T> extends WorkloadOperation<T> {

    protected static final Logger logger = LoggerFactory.getLogger(SplitOp.class);
    protected static final String PARAM_N = "n";

    protected int numOutputStreams;

    public SplitOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.numOutputStreams = ((Double) transformation.getParams().getOrDefault(PARAM_N, 0)).intValue();

        if(numOutputStreams < 1) {
            throw new StreamBenchException("Cannot split to non-positive number of output streams");
        }

        logger.info("New split operation with numOutputStreams=" + numOutputStreams);
    }
}
