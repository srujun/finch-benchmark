package streambench;

import com.google.common.graph.Network;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.WorkloadParser;
import streambench.workload.pojo.WorkloadTransformation;
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
            WorkloadConfig workloadConfig = WorkloadParser.getWorkloadConfig(new FileReader(workloadFilePath));
            Network<String, String> workloadNetwork = WorkloadParser.getWorkloadAsNetwork(new FileReader(workloadFilePath));

            Map<String, MessageStream<KV<String, String>>> msgStreams = new HashMap<>();
            Set<String> seenTransformations = new HashSet<>();

            workloadConfig.getSources().forEach(
                (srcname, sourceObj) -> {
                    logger.info("Source stream: " + srcname);
                    // Input message streams
                    msgStreams.put(srcname, graph.getInputStream(srcname));

                    Queue<String> transformations = new LinkedList<>();
                    transformations.addAll(workloadNetwork.successors(srcname));

                    // BFS Traversal
                    while(!transformations.isEmpty()) {
                        String name = transformations.remove();
                        logger.info("Transformation: " + name);
                        if(seenTransformations.contains(name)) {
                            logger.warn("Have seen, will skip...");
                            continue;
                        }
                        seenTransformations.add(name);

                        WorkloadTransformation transformation = workloadConfig.getTransformations().get(name);
                        if(transformation == null) {
                            logger.warn("Is null, will skip...");
                            continue;
                        }

                        List<MessageStream<KV<String, String>>> srcStreams = new ArrayList<>();
                        for(String pred : workloadNetwork.predecessors(name)) {
                            String streamName = workloadNetwork.edgeConnecting(pred, name).get();
                            logger.info("Src: " + pred + " streamName: " + streamName);
                            srcStreams.add(msgStreams.get(streamName));
                        }

                        // apply the transformation
                        ArrayList<MessageStream<KV<String, String>>> outStreams = WorkloadOperation.apply(transformation, srcStreams);

                        if (outStreams.size() > 1) {
                            for (int idx = 0; idx < outStreams.size(); idx++) {
                                msgStreams.put(transformation.getOutputs().get(idx), outStreams.get(idx));
                                logger.info("Putting transformation output " + transformation.getOutputs().get(idx));
                            }
                        } else {
                            msgStreams.put(name, outStreams.get(0));
                            logger.info("Putting transformation output " + name);
                        }

                        // add the successors to the queue
                        logger.info("successors(" + name + "): " + workloadNetwork.successors(name));
                        transformations.addAll(workloadNetwork.successors(name));
                    }
                }
            );

            // Send to outputstreams
            workloadConfig.getSinks().forEach(
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
