package streambench.workload;

import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streambench.workload.pojo.WorkloadConfig;

import java.io.FileReader;
import java.util.*;

/**
 * A class of helper functions to use to parse a given workload into different types of formats.
 */
public abstract class WorkloadParser {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadParser.class);

    public static WorkloadConfig getWorkloadConfig(FileReader workloadFile) {
        Gson gson = new Gson();
        return gson.fromJson(workloadFile, WorkloadConfig.class);
    }

    public static ImmutableNetwork<String, String> getWorkloadAsNetwork(FileReader workloadFile) {
        return getWorkloadAsNetwork(getWorkloadConfig(workloadFile));
    }

    public static ImmutableNetwork<String, String> getWorkloadAsNetwork(WorkloadConfig workloadConfig) {
        MutableNetwork<String, String> network = NetworkBuilder.directed().allowsSelfLoops(true).build();

        /* Add the sources and sinks as nodes */
        workloadConfig.getSources().forEach((name, src) -> network.addNode(name));
        workloadConfig.getSinks().forEach(network::addNode);

        /* Build the transformations into edges */
        workloadConfig.getTransformations().forEach(
                (name, transformation) -> {
                /* Get a list of inputs to this transformation */
                    List<String> streamInputs = new ArrayList<>();
                    if(transformation.getInput() != null)
                        streamInputs.add(transformation.getInput());
                    else
                        streamInputs.addAll(transformation.getInputs());

                    for(String srcStreamName : streamInputs) {
                        String srcNode = srcStreamName;
                        if (srcNode.contains("__"))
                            srcNode = srcNode.substring(0, srcNode.lastIndexOf("__"));

                        network.addEdge(srcNode, name, srcStreamName);

                        if (transformation.getOutputs() != null) {
                            for (String outputName : transformation.getOutputs()) {
                                if (workloadConfig.getSinks().contains(outputName)) {
                                    // the output is a sink
                                    network.addEdge(name, outputName, outputName);
                                }
                            }
                        }
                    }
                }
        );

        return ImmutableNetwork.copyOf(network);
    }

    public abstract Map<String, String> getWorkloadOptions(FileReader workloadFile);
}
