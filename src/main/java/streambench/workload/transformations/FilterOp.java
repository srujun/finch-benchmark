package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.Random;

public abstract class FilterOp<T> extends WorkloadOperation<T> {

    protected static final Logger logger = LoggerFactory.getLogger(FilterOp.class);
    protected static final String PARAM_P = "p";

    protected static Random rand;

    protected double dropProbability;

    static {
        rand = new Random();
        rand.setSeed(7762);
    }

    public FilterOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.dropProbability = (double) transformation.getParams().getOrDefault(PARAM_P, 0.5);

        logger.info("New filter operation with prob=" + dropProbability);
    }
}
