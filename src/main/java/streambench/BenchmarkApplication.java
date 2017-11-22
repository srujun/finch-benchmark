package streambench;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


public class BenchmarkApplication implements StreamApplication {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkApplication.class);

    // Inputs
    private static final String RAND_INPUT_STREAM_ID = "rand-input";

    // Outputs
    private static final String RAND_OUTPUT_STREAM_ID = "rand-output";

    private Random rand;

    public BenchmarkApplication() {

    }

    @Override
    public void init(StreamGraph graph, Config config) {
//        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));

        // Input message stream
        MessageStream<KV<String, String>> randomInputStream = graph.getInputStream(RAND_INPUT_STREAM_ID);

        // Output message stream
        OutputStream<KV<String, String>> randomOutputStream = graph.getOutputStream(RAND_OUTPUT_STREAM_ID);

        rand = new Random();
        rand.setSeed(7762);

        randomInputStream
                .map(kv -> {
                    logger.info("Got: " + kv.getKey() + ":" + kv.getValue());
                    return kv;
                })
                .filter(kv -> rand.nextBoolean())
                .sendTo(randomOutputStream);
    }

}
