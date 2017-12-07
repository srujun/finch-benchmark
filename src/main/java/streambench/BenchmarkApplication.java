package streambench;

import com.google.gson.Gson;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.transformations.WorkloadOperation;
import streambench.workload.pojo.WorkloadConfig;

import java.io.FileReader;
import java.net.URI;
import java.util.*;


public class BenchmarkApplication implements StreamApplication {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkApplication.class);
    /* load generation reference: https://caffinc.github.io/2016/03/cpu-load-generator/ */
    private static final String WORKLOAD_FILE_KEY = "streambench.workload.path";

    @Override
    public void init(StreamGraph graph, Config config) {
        try {
            String workloadFilePath = new URI(config.get(WORKLOAD_FILE_KEY)).getPath();
            Gson gson = new Gson();
            WorkloadConfig workloadConfig = gson.fromJson(new FileReader(workloadFilePath), WorkloadConfig.class);

            Map<String, MessageStream<KV<String, String>>> msgStreams = new HashMap<>();
//            Map<String, MessageStream<KV<String, String>>> dependentStreams = new HashMap<>();
//            Map<String, OutputStream<KV<String, String>>> outputStreams = new HashMap<>();

            Set<String> dependentStreamsSet = new HashSet<>();
            Set<String> outputStreamsSet = new HashSet<>();

            // Input message streams
            workloadConfig.getSources().forEach(
                (name, source) -> msgStreams.put(name, graph.getInputStream(name))
            );

            // Find which transformations have an outgoing edge
            workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                    dependentStreamsSet.add(transformation.getInput());
                }
            );

            logger.info("init streams=" + msgStreams.keySet());
            logger.info("dependent streams=" + dependentStreamsSet);

            workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                    String srcStreamName = transformation.getInput();
                    logger.info("Trying to set up " + srcStreamName + "->" + name);

                    if(!msgStreams.containsKey(srcStreamName)) {
                        // srcStream hasn't been defined yet
                        logger.error("Stream " + srcStreamName + " has not been defined!");
                        return;
                    }
                    MessageStream<KV<String, String>> srcStream = msgStreams.get(srcStreamName);

                    // apply the transformation
                    List<MessageStream<KV<String, String>>> outStreams = WorkloadOperation.apply(transformation, srcStream);
                    if(outStreams.size() > 1) {
                        for (int idx = 0; idx < outStreams.size(); idx++) {
                            msgStreams.put(name + "__" + (idx + 1), outStreams.get(idx));
                        }
                    } else {
                        msgStreams.put(name, outStreams.get(0));
                    }
                }
            );

            // create output streams
            outputStreamsSet.addAll(msgStreams.keySet());
            outputStreamsSet.removeAll(dependentStreamsSet);
            logger.info("output streams=" + outputStreamsSet);

            // Send to outputstreams
            outputStreamsSet.forEach(
                name -> {
                    OutputStream<KV<String, String>> oStream = graph.getOutputStream(name);
                    msgStreams.get(name).sendTo(oStream);
                }
            );
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Could not create stream graph: " + e.getMessage());
            for(StackTraceElement ste : e.getStackTrace())
                logger.error(ste.toString());
            throw new SamzaException(e);
        }
    }

}
