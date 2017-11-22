package streambench.system;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class BenchmarkInputMessageConsumer extends BlockingEnvelopeMap implements MessageFeed {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkInputMessageConsumer.class);

    private final static int NUM_KEYS = 10;
    private final static double ZIPF_PARAM = 1.2;
    private final String[] keys;
    private String systemName;
    private String streamName;

    private ScheduledFuture<?> emitterHandle;
    private final ScheduledExecutorService schedulerService;

    private MessageEmitter messageEmitter;

    public BenchmarkInputMessageConsumer(String systemName, Config systemConfig) {
        this.systemName = systemName;
//        this.streamName = systemConfig.get("stream.name");
        this.streamName = "rand-input";
        this.schedulerService = Executors.newSingleThreadScheduledExecutor();

        this.messageEmitter = new MessageEmitter(this, ZIPF_PARAM);

        IntStream keyRange = IntStream.range(0, NUM_KEYS);
        keys = keyRange.mapToObj(i -> "key" + i).toArray(String[]::new);
        logger.info("Keys length = " + keys.length);
    }

    @Override
    public void start() {
        logger.info("Starting BenchmarkInputMessageConsumer...");
        emitterHandle = schedulerService.scheduleAtFixedRate(messageEmitter,
                0, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        logger.info("Stopping BenchmarkInputMessageConsumer...");
        emitterHandle.cancel(true);
        // schedulerService.shutdown();
    }

    @Override
    public void sendMessage(String key, String value) {
        logger.info(String.format("sendMessage called with [%s, %s]", key, value));

        SystemStreamPartition partition = new SystemStreamPartition(systemName, streamName, new Partition(0));
        IncomingMessageEnvelope msgEnvelope = new IncomingMessageEnvelope(partition, null, key, value);

        try {
            put(partition, msgEnvelope);
        } catch (InterruptedException e) {
            logger.warn("FAILED TO SEND MESSAGE");
            logger.error(e.toString());
        }
    }

    class MessageEmitter implements Runnable {
        private MessageFeed feed;
        private final ZipfDistribution zipf;

        MessageEmitter(MessageFeed feedConsumer, double zipfParam) {
            this.feed = feedConsumer;
            this.zipf = new ZipfDistribution(NUM_KEYS, zipfParam);

            logger.info("MessageEmitter created!");
        }

        @Override
        public void run() {
            int index = zipf.sample() - 1;
            feed.sendMessage(keys[index], Instant.now().toString());
        }
    }
}
