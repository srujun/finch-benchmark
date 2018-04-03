package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.WindowConfig;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.WindowOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HeronWindowOp extends WindowOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    HeronWindowOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        Streamlet<KeyValue<String, String>> srcStream = srcStreams.get(0);
        Streamlet<KeyValue<String, String>> outStream = null;

        switch (this.windowType) {
            case TUMBLING:
                outStream = srcStream
                    .reduceByKeyAndWindow(
                        KeyValue::getKey,
                        WindowConfig.TumblingTimeWindow(windowDuration),
                        "",
                        (currValue, newMsg) -> newMsg.getValue())
                    .map(
                        windowKeyValue -> KeyValue.create(windowKeyValue.getKey().getKey(), windowKeyValue.getValue()));
                break;
            case SESSION:
                outStream = srcStream
                    .reduceByKeyAndWindow(
                        KeyValue::getKey,
                        WindowConfig.SlidingTimeWindow(windowDuration, windowDuration),
                        "",
                        (currValue, newMsg) -> newMsg.getValue())
                    .map(
                        windowKeyValue -> KeyValue.create(windowKeyValue.getKey().getKey(), windowKeyValue.getValue()));
                break;
        }

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
