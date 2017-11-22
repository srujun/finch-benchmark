package streambench.workload;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WorkloadOperation {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadOperation.class);

//    public static MessageStream<KV<String, String>> getOperation(String op, MessageStream<KV<String, String> inputStream) {
//        switch (op) {
//            case "filter":
//                return inputStream.filter(msg -> rand.nextBoolean()));
//                break;
//            default: logger.warn("Unknown operator: " + transformation.getOperator());
//        }
//    }

//    public abstract MessageStream<KV<String, String>> apply()
}
