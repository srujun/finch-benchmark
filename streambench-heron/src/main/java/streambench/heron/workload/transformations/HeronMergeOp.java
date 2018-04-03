package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.MergeOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HeronMergeOp extends MergeOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    HeronMergeOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        LinkedList<Streamlet<KeyValue<String, String>>> srcList = (LinkedList<Streamlet<KeyValue<String, String>>>) srcStreams;
        Streamlet<KeyValue<String, String>> first = srcList.remove();

        Streamlet<KeyValue<String, String>> unioned = srcList.stream().reduce(first, Streamlet::union);
        return new ArrayList<>(Collections.singletonList(unioned));
    }
}
