package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.StreamBenchException;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.JoinOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SamzaJoinOp extends JoinOp<MessageStream<KV<String, String>>> {

    private static final Logger logger = LoggerFactory.getLogger(SamzaJoinOp.class);

    SamzaJoinOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        logger.info("Found " + srcStreams.size() + " src streams");

        MessageStream<KV<String, String>> stream1 = srcStreams.get(0);
        MessageStream<KV<String, String>> stream2 = srcStreams.get(1);

        if(stream1 == null || stream2 == null) {
            logger.warn("Source streams are null");
            throw new StreamBenchException("Not enough source streams");
        }

        MessageStream<KV<String, String>> outStream = stream1.join(
            stream2, // join to stream2
            new StreamJoiner(), // the join function to use
            new StringSerde(), // the serde used for the join key
            KVSerde.of(new StringSerde(), new StringSerde()), // serde used for stream1
            KVSerde.of(new StringSerde(), new StringSerde()), // serde used for stream2
            this.ttl, // the TTL for messages in the stream
            this.name // the ID this operation will use in the state store (use the name of the transformation)
        );

        return new ArrayList<>(Collections.singletonList(outStream));
    }

    class StreamJoiner implements JoinFunction<String, KV<String, String>, KV<String, String>, KV<String, String>> {

        @Override
        public KV<String, String> apply(KV<String, String> msg1, KV<String, String> msg2) {
            assert msg1.getKey().equals(msg2.getKey());
            return new KV<>(msg1.getKey(), msg2.getValue() + msg2.getValue());
        }

        @Override
        public String getFirstKey(KV<String, String> msg) {
            return msg.getKey();
        }

        @Override
        public String getSecondKey(KV<String, String> msg) {
            return msg.getKey();
        }
    }
}
