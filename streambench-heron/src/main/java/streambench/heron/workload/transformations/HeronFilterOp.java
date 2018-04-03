package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.FilterOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HeronFilterOp extends FilterOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    HeronFilterOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        Streamlet<KeyValue<String, String>> srcStream = srcStreams.get(0);
        Streamlet<KeyValue<String, String>> outStream = srcStream.filter(msg -> (rand.nextDouble() <= dropProbability));

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
