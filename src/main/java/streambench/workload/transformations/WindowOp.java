package streambench.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.util.ArrayList;
import java.util.List;

public class WindowOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(WindowOp.class);

    public WindowOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        logger.info("New window operation");
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        return null;
    }
}
