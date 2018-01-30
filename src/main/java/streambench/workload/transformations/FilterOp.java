package streambench.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FilterOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(FilterOp.class);
    private static final String PARAM_P = "p";

    private static Random rand;

    private double dropProbability;

    static {
        rand = new Random();
        rand.setSeed(7762);
    }

    public FilterOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.dropProbability = (double) transformation.getParams().getOrDefault(PARAM_P, 0.5);

        logger.info("New filter operation with prob=" + dropProbability);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);

        MessageStream<KV<String, String>> outStream =
                srcStream.filter(msg -> (rand.nextDouble() <= dropProbability));
        ArrayList<MessageStream<KV<String, String>>> list = new ArrayList<>();
        list.add(outStream);
        return list;
    }
}
