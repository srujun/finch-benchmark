package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.SplitOp;

import java.util.ArrayList;
import java.util.List;

public class SamzaSplitOp extends SplitOp<MessageStream<KV<String, String>>> {

    SamzaSplitOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);

        ArrayList<MessageStream<KV<String, String>>> outStreams = new ArrayList<>();

        for(int idx = 0; idx < numOutputStreams; idx++) {
            outStreams.add(srcStream.map(kv -> kv));
        }

        return outStreams;
    }
}
