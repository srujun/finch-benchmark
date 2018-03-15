package streambench.workload.transformations;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadTransformation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JoinOp extends WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(JoinOp.class);

    private static final String PARAM_TTL = "ttl";

    private Duration ttl;

    public JoinOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);

        this.ttl = parseDuration((String) transformation.getParams().get(PARAM_TTL));
        logger.info("New join operation with ttl=" + ttl.toString());
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> stream1 = srcStreams.get(0);
        MessageStream<KV<String, String>> stream2 = srcStreams.get(1);

        MessageStream<KV<String, String>> outStream = stream1.join(
            stream2, // join to stream2
            new StreamJoiner(), // the join function to use
            new StringSerde(), // the serde used for the join key
            KVSerde.of(new StringSerde(), new StringSerde()), // serde used for stream1
            KVSerde.of(new StringSerde(), new StringSerde()), // serde used for stream2
            this.ttl, // the TTL for messages in the stream
            this.name // the ID this operation will use in the state store (use the name of the transformation)
        );

        ArrayList<MessageStream<KV<String, String>>> outStreams = new ArrayList<>();
        outStreams.add(outStream);
        return outStreams;
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
