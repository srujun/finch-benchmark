package streambench.workload;

import com.google.common.graph.ImmutableNetwork;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.NetworkBuilder;
import com.google.gson.Gson;
import streambench.system.BenchmarkMessageFactory;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadNode;
import streambench.workload.pojo.WorkloadSink;
import streambench.workload.pojo.WorkloadStream;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkloadParser {

    public static WorkloadConfig getWorkloadConfig(FileReader workloadFile) {
        Gson gson = new Gson();
        WorkloadConfig workloadConfig = gson.fromJson(workloadFile, WorkloadConfig.class);
        return workloadConfig;
    }

    public static Map<String, String> getWorklaodOptions(FileReader workloadFile) {
        Map<String, String> options = new HashMap<>();

        WorkloadConfig workloadConfig = WorkloadParser.getWorkloadConfig(workloadFile);

        workloadConfig.getSources().forEach(
            (name, src) -> {
                System.out.println("keydist=" + src.getKey_dist_params());
                String key;
                String systemName = name + "-system";

                key = "systems." + systemName + ".samza.factory";
                options.put(key, BenchmarkMessageFactory.class.getCanonicalName());

                key = "streams." + name + ".samza.system";
                options.put(key, systemName);

                key = "streams." + name + ".samza.key.serde";
                options.put(key, "string");
                key = "streams." + name + ".samza.msg.serde";
                options.put(key, "string");
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

    public static ImmutableNetwork<String, String> getWorkloadAsNetwork(FileReader workloadFile) {
        WorkloadConfig workloadConfig = WorkloadParser.getWorkloadConfig(workloadFile);

        MutableNetwork<String, String> network = NetworkBuilder.directed().allowsSelfLoops(true).build();

        workloadConfig.getSources().forEach((name, src) -> network.addNode(name));
        workloadConfig.getSinks().forEach(network::addNode);

        workloadConfig.getTransformations().forEach(
            (name, transformation) -> {
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
}
