package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;

public class MergeOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(MergeOp.class);

    public MergeOp(WorkloadTransformation transformation) {
        super(transformation);

        logger.info("New merge operation");
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        ArrayList<MessageStream<KV<String, String>>> outStreams = new ArrayList<>();
        outStreams.add(MessageStream.mergeAll(srcStreams));
        return outStreams;
    }
}
