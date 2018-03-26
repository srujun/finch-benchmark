package streambench.workload;

import com.google.common.graph.Network;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.WorkloadOperation;

import java.io.FileReader;
import java.util.*;

public class SamzaWorkloadParser extends WorkloadParser {

    private static final Logger logger = LoggerFactory.getLogger(SamzaWorkloadParser.class);

    private static SamzaWorkloadParser instance;

    public static SamzaWorkloadParser instance() {
        if(instance == null) {
            instance = new SamzaWorkloadParser();
        }

        return instance;
    }

    public Map<String, String> getWorkloadOptions(FileReader workloadFile) {
        Map<String, String> options = new HashMap<>();

        WorkloadConfig workloadConfig = getWorkloadConfig(workloadFile);
        workloadConfig.getSources().forEach(
                (name, src) -> {
                /* Initialize the message producing system if being used */
                /*
                String systemName = name + "-system";
                options.put("systems." + systemName + ".samza.factory", BenchmarkMessageFactory.class.getCanonicalName());
                options.put("streams." + name + ".samza.system", systemName);
                 */

                /* Specify the serdes being used for the sources */
                    options.put("streams." + name + ".samza.key.serde", "string");
                    options.put("streams." + name + ".samza.msg.serde", "string");
                }
        );
        workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                    System.out.println("Transformation name: " + name);
                    System.out.println("Transformation operator: " + transformation.getOperator());
                    if(transformation.getInput() != null)
                        System.out.println("Transformation input: " + transformation.getInput());
                    else
                        System.out.println("Transformation inputs: " + transformation.getInputs());
                }
        );

        return options;
    }

    public void setupStreams(StreamGraph graph, String workloadFilePath) {
        try {
            WorkloadConfig workloadConfig = getWorkloadConfig(new FileReader(workloadFilePath));
            Network<String, String> workloadNetwork = getWorkloadAsNetwork(new FileReader(workloadFilePath));

            /* Traverse the Network (graph) to create the message streams */
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

                            ArrayList<MessageStream<KV<String, String>>> outStreams;
                            // apply the transformation
                            try {
                                outStreams = WorkloadOperation.apply(name, transformation, srcStreams);
                            } catch (SamzaException e) {
                                logger.warn("Source streams not set up, will continue");
                                continue;
                            }

                            seenTransformations.add(name);

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

            /* Send to output streams (sinks) */
            workloadConfig.getSinks().forEach(name -> msgStreams.get(name).sendTo(graph.getOutputStream(name)));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Could not create stream graph: " + e.getMessage());
            for(StackTraceElement ste : e.getStackTrace())
                logger.error(ste.toString());
            throw new SamzaException(e);
        }
    }
}
