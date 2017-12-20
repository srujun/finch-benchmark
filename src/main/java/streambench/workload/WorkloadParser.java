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
import java.util.HashMap;
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
                System.out.println("Transformation input: " + transformation.getInput());
            }
        );

        return options;
    }

    public static ImmutableNetwork<String, String> getWorkloadAsNetwork(FileReader workloadFile) {
        WorkloadConfig workloadConfig = WorkloadParser.getWorkloadConfig(workloadFile);

        MutableNetwork<String, String> network = NetworkBuilder.directed().build();

        workloadConfig.getSources().forEach((name, src) -> network.addNode(name));
        workloadConfig.getSinks().forEach(network::addNode);

        workloadConfig.getTransformations().forEach(
            (name, transformation) -> {
                String srcStreamName = transformation.getInput();
                if(srcStreamName.contains("__"))
                    srcStreamName = srcStreamName.substring(0, srcStreamName.lastIndexOf("__"));

                network.addEdge(srcStreamName, name, transformation.getInput());

                if(transformation.getOutputs() != null) {
                    for(String outputName : transformation.getOutputs()) {
                        if(workloadConfig.getSinks().contains(outputName)) {
                            // the output is a sink
                            network.addEdge(name, outputName, outputName);
                        }
                    }
                }
            }
        );

        return ImmutableNetwork.copyOf(network);
    }
}
