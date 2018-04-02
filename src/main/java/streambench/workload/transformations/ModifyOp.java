package streambench.workload.transformations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.Random;

public abstract class ModifyOp<T> extends WorkloadOperation<T> {

    protected static final Logger logger = LoggerFactory.getLogger(FilterOp.class);
    protected static final String PARAM_RATE_RATIO = "rate_ratio";
    protected static final String PARAM_SIZE_RATIO = "size_ratio";

    protected static Random rand;

    protected double rate_ratio;
    protected double size_ratio;

    static {
        rand = new Random();
        rand.setSeed(7762);
    }

    public ModifyOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.rate_ratio = (double) transformation.getParams().getOrDefault(PARAM_RATE_RATIO, 1.0);
        this.size_ratio = (double) transformation.getParams().getOrDefault(PARAM_SIZE_RATIO, 1.0);

        logger.info("New modify operation with rate_ratio=" + rate_ratio + " size_rate=" + size_ratio);
    }
}
