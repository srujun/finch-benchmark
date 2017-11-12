package streambench;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class SimpleWrite {
    final static String TOPIC = "rand-input";
    final static int NUM_KEYS = 10;

    final static ScheduledExecutorService schedulerService = Executors.newSingleThreadScheduledExecutor();

    final static Properties config = new Properties();

    public static void main(String args[]) {

        if(args.length != 1)
        {
            System.err.println("usage: SimpleWrite zipf_param");
            System.exit(1);
        }

//        Random rand = new Random();
//        rand.setSeed(7762);

        double zipfParam = Double.valueOf(args[0]);
        ZipfDistribution zipf = new ZipfDistribution(NUM_KEYS, zipfParam);

        IntStream keyRange = IntStream.range(0, NUM_KEYS);
        final String[] keys = keyRange.mapToObj(i -> "key" + i).toArray(String[]::new);

        config.put("bootstrap.servers", "localhost:9092");
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        final Runnable emitter = () -> {
//            int index = rand.nextInt(NUM_KEYS);
            int index = zipf.sample() - 1;  // adjust for returned range of [1, k]
            final ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC, keys[index], Instant.now().toString());
            producer.send(record);
            System.out.println("Sent " + record.key() + ":" + record.value());
        };

        /*
         * 10 messages per second
         * total time 300 seconds
         */
        final ScheduledFuture<?> emitterHandle = schedulerService.scheduleAtFixedRate(
                emitter, 0, 100, TimeUnit.MILLISECONDS);

        final ScheduledFuture<?> cancelHandle = schedulerService.schedule(() -> {
            emitterHandle.cancel(true);
            schedulerService.shutdown();
        }, 5, TimeUnit.MINUTES);

        try {
            cancelHandle.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Done! " + schedulerService.isTerminated());
    }
}
