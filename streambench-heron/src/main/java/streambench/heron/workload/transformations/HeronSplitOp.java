package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.SplitOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HeronSplitOp extends SplitOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    HeronSplitOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        Streamlet<KeyValue<String, String>> srcStream = srcStreams.get(0);
        return new ArrayList<>(srcStream.clone(numOutputStreams));
    }
}
