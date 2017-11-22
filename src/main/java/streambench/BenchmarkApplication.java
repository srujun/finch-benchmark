package streambench;

import com.google.gson.Gson;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadConfig;

import java.io.FileReader;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;


public class BenchmarkApplication implements StreamApplication {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkApplication.class);
    private static final long SLEEP_DURATION = 500; // 0.5 second
    private static final double SLEEP_LOAD = 0.8;
    /* load generation reference: https://caffinc.github.io/2016/03/cpu-load-generator/ */
    private static final String WORKLOAD_FILE_KEY = "streambench.workload.path";

    private Random rand;

    @Override
    public void init(StreamGraph graph, Config config) {
        try {
            String workloadFilePath = new URI(config.get(WORKLOAD_FILE_KEY)).getPath();
            Gson gson = new Gson();
            WorkloadConfig workloadConfig = gson.fromJson(new FileReader(workloadFilePath), WorkloadConfig.class);

            rand = new Random();
            rand.setSeed(7762);

            Map<String, MessageStream<KV<String, String>>> inputStreams = new HashMap<>();
//            Map<String, OutputStream<KV<String, String>>> outputStreams = new HashMap<>();
            Set<String> dependentStreamsSet = new HashSet<>();
            Set<String> outputStreamsSet = new HashSet<>();

            // Input message streams
            workloadConfig.getSources().forEach(
                (name, source) -> inputStreams.put(name, graph.getInputStream(name))
            );

            // Find which transformations have an outgoing edge
            workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                    dependentStreamsSet.add(transformation.getInput());
                }
            );

            logger.info("init input streams=" + inputStreams.keySet());
            logger.info("dependent streams=" + dependentStreamsSet);

            // Apply the tranformations
            workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                    MessageStream<KV<String, String>> input = inputStreams.get(transformation.getInput());
                    switch (transformation.getOperator()) {
                        case "filter":
                            inputStreams.put(name, input
                                    .map(kv -> {
                                        long startTime = System.currentTimeMillis();
                                        try {
                                            while (System.currentTimeMillis() - startTime < SLEEP_DURATION)
                                                Thread.sleep((long) Math.floor((1 - SLEEP_LOAD) * 100));
                                        } catch (InterruptedException e) {

                                        }
                                        return kv;
                                    })
                                    .filter(msg -> rand.nextBoolean()));
                            break;
                        default: logger.warn("Unknown operator: " + transformation.getOperator());
                    }
                }
            );

            logger.info("final input streams=" + inputStreams.keySet());

            // create output streams
            outputStreamsSet.addAll(inputStreams.keySet());
            outputStreamsSet.removeAll(dependentStreamsSet);
            logger.info("output streams=" + outputStreamsSet);

            // Send to outputstreams
            outputStreamsSet.forEach(
                name -> {
                    OutputStream<KV<String, String>> oStream = graph.getOutputStream(name);
                    inputStreams.get(name).sendTo(oStream);
                }
            );

//            MessageStream<KV<String, String>> randomInputStream = graph.getInputStream("rand-input");
//            OutputStream<KV<String, String>> randomOutputStream = graph.getOutputStream("rand-output");
//            OutputStream<String> statsStream = graph.getOutputStream("_streambench-stats", new StringSerde());
//
//            randomInputStream
//                    .filter(kv -> rand.nextBoolean())
//                    .sendTo(randomOutputStream);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Could not create stream graph: " + e.getMessage());
            for(StackTraceElement ste : e.getStackTrace())
                logger.error(ste.toString());
            throw new SamzaException(e);
        }
    }

}
