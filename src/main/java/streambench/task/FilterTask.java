package streambench.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Random;

public class FilterTask implements StreamTask {

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "rand-output");

    private static final Random rand = new Random();

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String key = (String) envelope.getKey();
        String message = (String) envelope.getMessage();

        System.err.print("Got " + key + ":[" + message + "]");

        if(rand.nextBoolean()) {
            System.err.println();
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, key, message));
        } else {
            System.err.println(" => Dropped!");
        }
    }
}
