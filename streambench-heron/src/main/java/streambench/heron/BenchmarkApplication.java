package streambench.heron;

import com.twitter.heron.streamlet.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import streambench.heron.workload.HeronWorkloadParser;
import streambench.workload.pojo.WorkloadConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class BenchmarkApplication {

    private static final Logger LOG = Logger.getLogger(BenchmarkApplication.class.getName());

    private static final String WORKLOAD_OPT = "workload-path";
    private static final String PROPERTIES_OPT = "properties-path";

    public static void main(String[] args) throws IOException {
        FileHandler fileHandler = new FileHandler("benchmark-application.log");
        LOG.addHandler(fileHandler);

        WorkloadConfig workloadConfig = null;
        try {
            workloadConfig = getWorkloadConfigFromArgs(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        List<String> bootstrapServers = Arrays.asList("ip-172-31-23-57:9092", "ip-172-31-27-22:9092");

        Builder builder = Builder.newBuilder();
        System.out.println("Setting up Heron Streamlets...");
        HeronWorkloadParser.instance().setupStreams(builder, workloadConfig, bootstrapServers);

//        Config config = Config.newBuilder()
//                .setNumContainers(10)
//                .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
//                .setSerializer(Config.Serializer.KRYO)
//                .build();

        Config config = Config.defaultConfig();

        new Runner().run("HeronBenchmark", config, builder);
    }

    private static WorkloadConfig getWorkloadConfigFromArgs(String[] fromArgs) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> workloadOpt = parser.accepts(WORKLOAD_OPT, "path to workload JSON file")
                .withRequiredArg()
                .ofType(File.class)
                .required();
        OptionSpec<File> propertiesOpt = parser.accepts(PROPERTIES_OPT, "path to Samza .properties file")
                .withRequiredArg()
                .ofType(File.class)
                .required();
        OptionSpec<Void> helpOpt = parser.acceptsAll(Arrays.asList("h", "help"), "Show help")
                .forHelp();

        OptionSet options = parser.parse(fromArgs);
        if(options.has(helpOpt) || (!options.has(workloadOpt) || !options.has(propertiesOpt))) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        FileReader workloadReader;
        FileReader propertiesReader;

        try {
            workloadReader = new FileReader(options.valueOf(workloadOpt));
        } catch (FileNotFoundException e) {
            System.err.println("File " + options.valueOf(workloadOpt).getPath() + " not found!");
            throw e;
        }
        try {
            propertiesReader = new FileReader(options.valueOf(propertiesOpt));
        } catch (FileNotFoundException e) {
            System.err.println("File " + options.valueOf(propertiesOpt).getPath() + " not found!");
            throw e;
        }

        return HeronWorkloadParser.getWorkloadConfig(workloadReader);
    }
}
