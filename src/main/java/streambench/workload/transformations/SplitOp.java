package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;

public class SplitOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(SplitOp.class);
    private static final String PARAM_N = "n";

    private int numOutputStreams;

    public SplitOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
        this.numOutputStreams = ((Double) transformation.getParams().getOrDefault(PARAM_N, 0)).intValue();

        if(numOutputStreams < 1) {
            throw new SamzaException("Cannot split to non-positive number of output streams");
        }

        logger.info("New split operation with numOutputStreams=" + numOutputStreams);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);

        ArrayList<MessageStream<KV<String, String>>> outStreams = new ArrayList<>();

        for(int idx = 0; idx < numOutputStreams; idx++) {
            int finalIdx = idx;
            outStreams.add(
                srcStream.map(
                    kv -> {
                        String key = kv.getKey();
                        String val = kv.getValue();
                        int valLength = val.length() / numOutputStreams;
                        return new KV<>(key, val.substring(finalIdx*valLength, (finalIdx+1)*valLength));
                    }
                )
            );
        }

        return outStreams;
    }
}
