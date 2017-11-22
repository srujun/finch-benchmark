package streambench.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMessageFactory implements SystemFactory {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkMessageFactory.class);

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry metricsRegistry) {
        logger.info("Getting BenchmarkMessageFactory consumer");
        return new BenchmarkInputMessageConsumer(systemName, config.subset("streambench.workload." + systemName));
    }

    @Override
    public SystemProducer getProducer(String s, Config config, MetricsRegistry metricsRegistry) {
        logger.info("Getting BenchmarkMessageFactory producer");
        throw new SamzaException("Cannot produce to BenchmarkMessage system");
    }

    @Override
    public SystemAdmin getAdmin(String s, Config config) {
        return new SinglePartitionWithoutOffsetsSystemAdmin();
    }
}
