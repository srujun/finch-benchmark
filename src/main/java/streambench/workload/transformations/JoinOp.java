package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.time.Duration;

public abstract class JoinOp<T> extends WorkloadOperation<T> {

    protected static final Logger logger = LoggerFactory.getLogger(JoinOp.class);

    protected static final String PARAM_TTL = "ttl";

    protected Duration ttl;

    public JoinOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        this.ttl = parseDuration((String) transformation.getParams().get(PARAM_TTL));
        logger.info("New join operation with ttl=" + ttl.toString());
    }
}
