package streambench.producer;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import streambench.workload.WorkloadParser;
import streambench.workload.pojo.WorkloadConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class CoreApp {
    private static final String WORKLOAD_OPT = "workload-path";
    private static final String KAFKA_OPT = "kafka";
    private static final String PARTITIONS_OPT = "partitions";
    private static final String INFLUX_OPT = "influx";
    private static final String DB_OPT = "db";

    private static final List<String> DEFAULT_KAFKA = Arrays.asList("ip-172-31-5-82:9092", "ip-172-31-6-235:9092");
    private static final String DEFAULT_INFLUXDB_DBNAME = "latency-metrics";

    private OptionSpec<Void> helpOpt;
    private OptionSpec<File> workloadOpt;
    private OptionSpec<String> kafkaServerOpt;
    private OptionSpec<Integer> partitionsOpt;
    private OptionSpec<String> influxServerOpt;
    private OptionSpec<String> dbOpt;

    public static void main(String[] args) throws Exception {
        CoreApp app = new CoreApp();

        OptionSet cmdLineOpts = app.getArgs(args);

        WorkloadConfig workloadConfig = app.getWorkload(cmdLineOpts.valueOf(app.workloadOpt));
        List<String> kafkaBrokers;
        if(cmdLineOpts.has(app.kafkaServerOpt))
            kafkaBrokers = cmdLineOpts.valuesOf(app.kafkaServerOpt);
        else
            kafkaBrokers = DEFAULT_KAFKA;
        int partitions = cmdLineOpts.valueOf(app.partitionsOpt);
        String influxServer = cmdLineOpts.valueOf(app.influxServerOpt);
        String db = cmdLineOpts.valueOf(app.dbOpt);

        /* create executor thread pool of size = num of topics to write to + sink topics to read from */
        int threadPoolSize = workloadConfig.getSources().size() + workloadConfig.getSinks().size();
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(threadPoolSize);

        SimpleProducer producer = new SimpleProducer(kafkaBrokers, workloadConfig, partitions, executorService);
        LatencyToInflux latency = new LatencyToInflux(kafkaBrokers, workloadConfig, influxServer, db, executorService);
    }

    private OptionSet getArgs(String[] fromArgs) throws IOException {
        OptionParser parser = new OptionParser();
        helpOpt = parser.acceptsAll(Arrays.asList("h", "help"), "Show help")
                .forHelp();

        workloadOpt = parser.accepts(WORKLOAD_OPT, "path to workload JSON file")
                .requiredUnless(helpOpt)
                .withRequiredArg()
                .ofType(File.class);
        kafkaServerOpt = parser.accepts(KAFKA_OPT, "Kafka bootstrap servers")
                .withRequiredArg()
                .withValuesSeparatedBy(",");
        partitionsOpt = parser.accepts(PARTITIONS_OPT, "number of source partitions")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(1);

        influxServerOpt = parser.accepts(INFLUX_OPT, "InfluxDB server")
                .requiredUnless(helpOpt)
                .withRequiredArg();
        dbOpt = parser.accepts(DB_OPT, "database to write latency metrics to")
                .withRequiredArg()
                .defaultsTo(DEFAULT_INFLUXDB_DBNAME);

        OptionSet options = parser.parse(fromArgs);

        if(options.has(helpOpt)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }
        return options;
    }

    private WorkloadConfig getWorkload(File workloadFile) throws FileNotFoundException {
        FileReader workloadReader;

        try {
            workloadReader = new FileReader(workloadFile);
        } catch (FileNotFoundException e) {
            System.err.println("File " + workloadFile.getPath() + " not found!");
            throw e;
        }

        return WorkloadParser.getWorkloadConfig(workloadReader);
    }
}
