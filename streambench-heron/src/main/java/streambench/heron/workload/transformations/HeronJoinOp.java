package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.WindowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.JoinOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HeronJoinOp extends JoinOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HeronJoinOp.class);

    HeronJoinOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        Streamlet<KeyValue<String, String>> stream1 = srcStreams.get(0);
        Streamlet<KeyValue<String, String>> stream2 = srcStreams.get(1);

        if(stream1 == null || stream2 == null) {
            logger.warn("Source streams are null");
            throw new StreamBenchException("Not enough source streams");
        }

        Streamlet<KeyValue<KeyedWindow<String>, String>> keyedStream = stream1.join(
                stream2,
                KeyValue::getKey,
                KeyValue::getValue,
                WindowConfig.TumblingTimeWindow(this.ttl),
                (msg1, msg2) -> msg1.getValue() + msg2.getValue()
        );

        Streamlet<KeyValue<String, String>> outStream = keyedStream.map(
                windowKeyValue -> KeyValue.create(windowKeyValue.getKey().getKey(), windowKeyValue.getValue()));

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
