package streambench.heron.workload.tests;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.*;
import streambench.heron.KafkaSink;
import streambench.heron.KafkaSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class Pipeline {

    public static void main(String[] args) throws IOException {
        List<String> bootstrapServers = Arrays.asList("ip-172-31-5-82:9092", "ip-172-31-6-235:9092");

        Random rand = new Random();

        Builder builder = Builder.newBuilder();
        System.out.println("Setting up Heron Streamlets...");

        Streamlet<KeyValue<String, String>> source =
                builder.newSource(new KafkaSource(bootstrapServers, "source1"))
                       .setName("source1")
                       .setNumPartitions(10);

        List<Streamlet<KeyValue<String, String>>> clones = source
            .filter(msg -> (rand.nextDouble() <= 0.5)).setName("filter1")
            .clone(2); // creates 2 map functions

        Streamlet<KeyValue<String, String>> stream1 = clones.get(0)
            .filter(msg -> (rand.nextDouble() <= 0.75)).setName("filter2");

        final double size_ratio = 1.5;
        Streamlet<KeyValue<String, String>> stream2 = clones.get(1)
            .map(msg -> {
                String key = msg.getKey();
                String val = msg.getValue();
//                StringBuilder finalValBuilder = new StringBuilder();
//                int size_whole = (new Double(Math.floor(size_ratio))).intValue();
//                double size_frac = size_ratio - Math.floor(size_ratio);
//
//                for(int i = 0; i < size_whole; i++) {
//                    finalValBuilder.append(val);
//                }
//                finalValBuilder.append(val.substring(0, (new Double(val.length() * size_frac)).intValue()));
                return new KeyValue<>(key, val + "abcdefghijklm");
            }).setName("cmap1");

        stream1
            .join(
                    stream2,
                    KeyValue::getKey,
                    KeyValue::getValue,
                    WindowConfig.TumblingTimeWindow(Duration.ofSeconds(5)),
                    (msg1, msg2) -> msg2.getValue() /*+ msg2.getValue()*/)
                .setName("join1")
            .map(windowKeyValue -> KeyValue.create(windowKeyValue.getKey().getKey(), windowKeyValue.getValue()))
                .setName("cmap2")
            .toSink(new KafkaSink(bootstrapServers, "sink1"));

        long bytes_256MB = ByteAmount.fromMegabytes(256).asBytes();
        List<String> components = Arrays.asList("source1", "filter1", "filter2", "map1", "map2", "cmap1", "cmap2", "join1", "sink1");
        components = components.stream().map(component -> component + ":" + bytes_256MB).collect(Collectors.toList());
        String ramMap = String.join(",", components);

        // Config config = Config.defaultConfig();
        Config config = Config.newBuilder()
                .setNumContainers(10)
                // .setPerContainerRamInMegabytes(512)
                .setSerializer(Config.Serializer.KRYO)
                .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
                .setUserConfig(com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_RAMMAP, ramMap)
                .build();

        new Runner().run("Pipeline", config, builder);
    }
}
