package streambench.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ModifyOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(FilterOp.class);
    private static final String PARAM_RATE_RATIO = "rate_ratio";
    private static final String PARAM_SIZE_RATIO = "size_ratio";

    private static Random rand;

    private double ratio;

    static {
        rand = new Random();
        rand.setSeed(7762);
    }

    public ModifyOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.ratio = (double) transformation.getParams().getOrDefault(PARAM_RATE_RATIO, 1.0);

        logger.info("New modify operation with ratio=" + ratio);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);

        // TODO: implement size modify

        MessageStream<KV<String, String>> outStream =
            srcStream.flatMap(msg -> {
                List<KV<String, String>> outMsgs = new ArrayList<>();
                for(int i = 0; i < (int) ratio; i++) {
                    outMsgs.add(msg);
                }

                double probability = ratio - Math.floor(ratio);
                if(rand.nextDouble() <= probability)
                    outMsgs.add(msg);

                return outMsgs;
            }
        );

        ArrayList<MessageStream<KV<String, String>>> list = new ArrayList<>();
        list.add(outStream);
        return list;
    }
}
