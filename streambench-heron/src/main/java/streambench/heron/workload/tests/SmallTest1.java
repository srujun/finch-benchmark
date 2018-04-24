package streambench.heron.workload.tests;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.*;
import com.twitter.heron.streamlet.Config;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class SmallTest1 {

    public static void main(String[] args) throws IOException {
        List<String> bootstrapServers = Arrays.asList("ip-172-31-5-82:9092", "ip-172-31-6-235:9092");

        Random rand = new Random();

        Builder builder = Builder.newBuilder();
        System.out.println("Setting up Heron Streamlets...");

        Streamlet<KeyValue<String, String>> source =
                builder.newSource(new KafkaSource(bootstrapServers, "source1"))
                       .setName("source1");

        source
            .setNumPartitions(4)
            .filter(msg -> (rand.nextDouble() <= 0.5)).setName("filter")
            .flatMap(msg -> Arrays.asList(msg, msg)).setName("flatmap")
            .toSink(new KafkaSink(bootstrapServers, "sink1"));

        long bytes_256MB = ByteAmount.fromMegabytes(256).asBytes();
        List<String> components = Arrays.asList("source1", "filter", "flatmap", "sink1");
        components = components.stream().map(component -> component + ":" + bytes_256MB).collect(Collectors.toList());
        String ramMap = String.join(",", components);

        // Config config = Config.defaultConfig();
        Config config = Config.newBuilder()
                .setNumContainers(4)
                .setPerContainerCpu(1f)
                // .setPerContainerRamInMegabytes(100)
                .setSerializer(Config.Serializer.KRYO)
                .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
                .setUserConfig(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP, ramMap)
                .build();

        new Runner().run("SmallTest1", config, builder);
    }
}
