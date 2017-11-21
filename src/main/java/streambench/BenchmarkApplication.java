package streambench;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;

import java.util.Random;


public class BenchmarkApplication implements StreamApplication {

    // Inputs
    private static final String RAND_INPUT_STREAM_ID = "rand-input";

    // Outputs
    private static final String RAND_OUTPUT_STREAM_ID = "rand-output";

    private Random rand;

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));

        // Input message stream
        MessageStream<KV<String, String>> randomInputStream = graph.getInputStream(RAND_INPUT_STREAM_ID);

        // Output message stream
        OutputStream<KV<String, String>> randomOutputStream = graph.getOutputStream(RAND_OUTPUT_STREAM_ID);

        rand = new Random();
        rand.setSeed(7762);

        randomInputStream
                .filter(s -> rand.nextBoolean())
                .sendTo(randomOutputStream);
    }

}
