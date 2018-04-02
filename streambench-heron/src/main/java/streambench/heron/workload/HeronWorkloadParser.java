package streambench.heron.workload;

import com.google.common.graph.Network;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;
import streambench.heron.workload.transformations.HeronWorkloadOperation;
import streambench.workload.WorkloadParser;
import streambench.workload.pojo.WorkloadConfig;
import streambench.workload.pojo.WorkloadTransformation;

import java.io.FileReader;
import java.util.*;

public class HeronWorkloadParser extends WorkloadParser {

    private static HeronWorkloadParser instance;

    public static HeronWorkloadParser instance() {
        if(instance == null) {
            instance = new HeronWorkloadParser();
        }

        return instance;
    }

    @Override
    public Map<String, String> getWorkloadOptions(FileReader fileReader) {
        return null;
    }

    public void setupStreams(Builder builder, WorkloadConfig workloadConfig, List<String> bootstrapServers) {
        Network<String, String> workloadNetwork = getWorkloadAsNetwork(workloadConfig);

        Map<String, Streamlet<KeyValue<String, String>>> streamlets = new HashMap<>();
        Set<String> seenStreamlets = new HashSet<>();

        /* traverse the network to configure transformations */
        workloadConfig.getSources().forEach(
            (srcName, src) -> {
                /* set up the Kafka sources */
                Streamlet<KeyValue<String, String>> source =
                        builder.newSource(new KafkaSource(bootstrapServers, srcName))
                                .setName(srcName);
                streamlets.put(srcName, source);

                Queue<String> transformations = new LinkedList<>();
                transformations.addAll(workloadNetwork.successors(srcName));

                /* BFS traversal */
                System.out.println("Starting BFS...");
                while(!transformations.isEmpty()) {
                    String name = transformations.remove();
                    System.out.println("\nGot transformation: " + name);
                    if(seenStreamlets.contains(name)) {
                        /* already processed this transformation, should skip */
                        System.out.println("Have seen, will skip!");
                        continue;
                    }

                    /* TODO: is null check really needed? */
                    WorkloadTransformation transformation = workloadConfig.getTransformations().get(name);
                    if(transformation == null) {
                        System.out.println("Is NULL, will skip!");
                        continue;
                    }

                    System.out.println("Transformation op: " + transformation.getOperator());

                    List<Streamlet<KeyValue<String, String>>> inputs = new ArrayList<>();
                    List<Streamlet<KeyValue<String, String>>> outputs;

                    /* get all inputs to this transformation */
                    System.out.println("Inputs: " + workloadNetwork.predecessors(name));
                    workloadNetwork.predecessors(name).forEach(inputName -> {
                        String streamName = workloadNetwork.edgeConnecting(inputName, name).get();
                        inputs.add(streamlets.get(streamName));
                    });

                    /* apply the transformation to the inputs and get the outputs */
                    outputs = HeronWorkloadOperation.apply(name, transformation, inputs);

                    seenStreamlets.add(name);

                    if (outputs.size() > 1) {
                        for (int idx = 0; idx < outputs.size(); idx++) {
                            streamlets.put(transformation.getOutputs().get(idx), outputs.get(idx));
                        }
                    } else {
                        streamlets.put(name, outputs.get(0));
                    }

                    /* add successors to the queue */
                    transformations.addAll(workloadNetwork.successors(name));
                }
            }
        );

        /* Send to output streams (sinks) */
        workloadConfig.getSinks().forEach(name -> streamlets.get(name).toSink(new KafkaSink(bootstrapServers, name)));
    }
}
