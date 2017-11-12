package streambench.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

public class BenchmarkMessageFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String s, Config config, MetricsRegistry metricsRegistry) {
        return null;
    }

    @Override
    public SystemProducer getProducer(String s, Config config, MetricsRegistry metricsRegistry) {
        throw new SamzaException("Cannot produce to BenchmarkMessage system");
    }

    @Override
    public SystemAdmin getAdmin(String s, Config config) {
        return new SinglePartitionWithoutOffsetsSystemAdmin();
    }
}
