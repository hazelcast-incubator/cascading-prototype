package org;

import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.core.HazelcastInstance;

public class Util {
    public static void warmUpPartitions(HazelcastInstance... instances) throws InterruptedException {
        for (HazelcastInstance instance : instances) {
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    Thread.sleep(10);
                }
            }
        }
    }
}
