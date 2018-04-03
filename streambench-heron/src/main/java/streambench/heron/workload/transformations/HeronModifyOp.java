package streambench.heron.workload.transformations;

import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Streamlet;
import streambench.workload.pojo.WorkloadTransformation;
import streambench.workload.transformations.ModifyOp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HeronModifyOp extends ModifyOp<Streamlet<KeyValue<String, String>>> implements Serializable {

    HeronModifyOp(String name, WorkloadTransformation transformation) {
        super(name, transformation);
    }

    @Override
    public ArrayList<Streamlet<KeyValue<String, String>>> apply(List<Streamlet<KeyValue<String, String>>> srcStreams) {
        Streamlet<KeyValue<String, String>> srcStream = srcStreams.get(0);

        Streamlet<KeyValue<String, String>> outStream = srcStream
                /* rate modify */
                .flatMap(msg -> {
                    List<KeyValue<String, String>> outMsgs = new ArrayList<>();
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
                    finalValBuilder.append(val.substring(0, (new Double(val.length() * size_frac)).intValue()));
                    return new KeyValue<>(key, finalValBuilder.toString());
                });

        return new ArrayList<>(Collections.singletonList(outStream));
    }
}
