package streambench.samza.workload.transformations;

import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.ModifyOp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SamzaModifyOp extends ModifyOp<MessageStream<KV<String, String>>> {

    SamzaModifyOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<MessageStream<KV<String, String>>> apply(List<MessageStream<KV<String, String>>> srcStreams) {
        MessageStream<KV<String, String>> srcStream = srcStreams.get(0);

        MessageStream<KV<String, String>> outStream = srcStream
                /* rate modify */
                .flatMap(msg -> {
                    List<KV<String, String>> outMsgs = new ArrayList<>();
                    for(int i = 0; i < (int) rate_ratio; i++) {
                        outMsgs.add(msg);
                    }

                    double probability = rate_ratio - Math.floor(rate_ratio);
                    if(rand.nextDouble() <= probability)
                        outMsgs.add(msg);

                    return outMsgs;
                })
                /* size modify */
                .map(kv -> {
                    String key = kv.getKey();
                    String val = kv.getValue();
                    StringBuilder finalValBuilder = new StringBuilder();
                    int size_whole = (new Double(Math.floor(size_ratio))).intValue();
                    double size_frac = size_ratio - Math.floor(size_ratio);

                    for(int i = 0; i < size_whole; i++) {
                        finalValBuilder.append(val);
                    }
                    finalValBuilder.append(val.substring(0, ((Double) (val.length() * size_frac)).intValue()));
                    return new KV<>(key, finalValBuilder.toString());
                });

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
