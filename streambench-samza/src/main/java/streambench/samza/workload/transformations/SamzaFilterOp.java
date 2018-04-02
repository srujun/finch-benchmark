package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.FilterOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SamzaFilterOp extends FilterOp<MessageStream<KV<String, String>>> {

    SamzaFilterOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);
        MessageStream<KV<String, String>> outStream = srcStream.filter(msg -> (rand.nextDouble() <= dropProbability));

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
