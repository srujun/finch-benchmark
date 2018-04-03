package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.io.Serializable;

public abstract class MergeOp<T> extends WorkloadOperation<T> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(MergeOp.class);

    public MergeOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        logger.info("New merge operation");
    }
}
