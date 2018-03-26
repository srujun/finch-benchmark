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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class BenchmarkApplication {

    private static final Logger LOG = Logger.getLogger(BenchmarkApplication.class.getName());

    private static final String WORKLOAD_OPT = "workload-path";

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

        List<String> bootstrapServers = Collections.singletonList("localhost:9092");

        Builder builder = Builder.newBuilder();
        HeronWorkloadParser.instance().setupStreams(builder, workloadConfig, bootstrapServers);

        Config config = Config.defaultConfig();

        new Runner().run("HeronBenchmark", config, builder);
    }

    private static WorkloadConfig getWorkloadConfigFromArgs(String[] fromArgs) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<File> workloadOpt = parser.accepts(WORKLOAD_OPT, "path to workload JSON file")
                .withRequiredArg()
                .ofType(File.class)
                .required();
        OptionSpec<Void> helpOpt = parser.acceptsAll(Arrays.asList("h", "help"), "Show help")
                .forHelp();

        OptionSet options = parser.parse(fromArgs);
        if(options.has(helpOpt) || !options.has(workloadOpt)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        FileReader workloadReader;

        try {
            workloadReader = new FileReader(options.valueOf(workloadOpt));
        } catch (FileNotFoundException e) {
            System.err.println("File " + options.valueOf(workloadOpt).getPath() + " not found!");
            throw e;
        }

        return HeronWorkloadParser.instance().getWorkloadConfig(workloadReader);
    }
}
