package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.MergeOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SamzaMergeOp extends MergeOp<MessageStream<KV<String, String>>> {

    SamzaMergeOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> outStream = MessageStream.mergeAll(srcStreams);
        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
