package streambench;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;

import java.util.Map;
import java.util.Random;

import joptsimple.OptionSet;

public class BenchmarkApplication implements StreamApplication {

    // Inputs
    private static final String RAND_INPUT_STREAM_ID = "rand-input";

    // Outputs
    private static final String RAND_OUTPUT_STREAM_ID = "rand-output";

    private Random rand;

    @Override
    public void init(StreamGraph graph, Config config) {
        System.out.println("Printing config");
        for(Map.Entry<?, ?> entry : config.entrySet())
            System.out.println("Config: " + entry.getKey() + "=" + entry.getValue());
        System.out.println();

        // Input message stream
        MessageStream<String> randomInputStream = graph.getInputStream(RAND_INPUT_STREAM_ID);

        // Output message stream
        OutputStream<String> randomOutputStream = graph.getOutputStream(RAND_OUTPUT_STREAM_ID);

        rand = new Random();
        rand.setSeed(7762);

        randomInputStream
//                .filter(s -> rand.nextBoolean())
                .map((String msg) -> {
                    System.err.println("Got message: " + msg);
                    return msg;
                })
                .sendTo(randomOutputStream);
    }

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        LocalApplicationRunner runner = new LocalApplicationRunner(config);
        BenchmarkApplication app = new BenchmarkApplication();

        runner.run(app);
        runner.waitForFinish();
    }
}
