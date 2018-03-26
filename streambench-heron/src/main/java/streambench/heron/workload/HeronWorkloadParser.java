package streambench.heron.workload;

import com.google.common.graph.Network;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Streamlet;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;
import streambench.workload.WorkloadParser;
import streambench.workload.pojo.WorkloadConfig;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        /* set up the Kafka sources */
        Map<String, Streamlet> sources = new HashMap<>();
        workloadConfig.getSources().forEach((srcName, src) -> {
            sources.put(srcName, builder.newSource(new KafkaSource(bootstrapServers, srcName))
                                        .setName(srcName));
        });

        /* traverse the network to configure transformations */
    }
}
