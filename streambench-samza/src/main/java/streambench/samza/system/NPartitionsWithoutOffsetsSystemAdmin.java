package streambench.samza.system;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NPartitionsWithoutOffsetsSystemAdmin implements SystemAdmin {

    private Map<Partition, SystemStreamPartitionMetadata> partitionMetadata;

    NPartitionsWithoutOffsetsSystemAdmin(int numPartitions) {
        partitionMetadata = new HashMap<>();

        for(int p = 0; p < numPartitions; p++)
            partitionMetadata.put(new Partition(p), new SystemStreamPartitionMetadata(null, null, null));
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        Map<SystemStreamPartition, String> offsetsAfter = new HashMap<>();

        for (SystemStreamPartition systemStreamPartition : offsets.keySet()) {
            offsetsAfter.put(systemStreamPartition, null);
        }

        return offsetsAfter;
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> metadata = new HashMap<>();

        for (String streamName : streamNames) {
            metadata.put(streamName, new SystemStreamMetadata(streamName, partitionMetadata));
        }

        return metadata;
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
        return null;
    }

}
