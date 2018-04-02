package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.StringSerde;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.WindowOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class SamzaWindowOp extends WindowOp<MessageStream<KV<String, String>>> {

    SamzaWindowOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);
        MessageStream<KV<String, String>> outStream = null;

        Function<KV<String, String>, String> keyFn = KV::getKey;
        Supplier<String> initialValue = () -> "";
        FoldLeftFunction<KV<String, String>, String> aggregator = (newMsg, oldValue) -> newMsg.getValue();

        switch (this.windowType) {
            case TUMBLING:
                outStream = srcStream
                        .window(
                                Windows.keyedTumblingWindow(
                                        keyFn,
                                        this.windowDuration,
                                        initialValue,
                                        aggregator,
                                        new StringSerde(),
                                        new StringSerde()),
                                this.name)
                        .map(windowPane -> KV.of(windowPane.getKey().getKey(), windowPane.getMessage()));
                break;
            case SESSION:
                outStream = srcStream
                        .window(
                                Windows.keyedSessionWindow(
                                        keyFn,
                                        this.windowDuration,
                                        initialValue,
                                        aggregator,
                                        new StringSerde(),
                                        new StringSerde()),
                                this.name)
                        .map(windowPane -> KV.of(windowPane.getKey().getKey(), windowPane.getMessage()));
                break;
        }

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
