package com.hazelcast.yarn.impl.tap.source;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tap.TapType;
import com.hazelcast.yarn.api.tap.SourceTap;
import com.hazelcast.yarn.api.tuple.TupleReader;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class HazelcastSourceTap extends SourceTap {
    private final String name;
    private final TapType tapType;

    public HazelcastSourceTap(String name, TapType tapType) {
        this.name = name;
        this.tapType = tapType;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public TapType getType() {
        return this.tapType;
    }

    public TupleReader[] getReaders(ApplicationContext applicationContext, Vertex vertex, TupleFactory tupleFactory) {
        List<TupleReader> readers = new ArrayList<TupleReader>();

        if (TapType.HAZELCAST_LIST == this.tapType) {
            int partitionId = HazelcastListPartitionReader.getPartitionId(applicationContext.getNodeEngine(), this.name);
            InternalPartition partition = applicationContext.getNodeEngine().getPartitionService().getPartition(partitionId);

            if ((partition != null) && (partition.isLocal())) {
                readers.add(HazelcastReaderFactory.getReader(this.tapType, this.name, applicationContext, partitionId, tupleFactory, vertex));
            } else {
                readers.add(HazelcastReaderFactory.getReader(this.tapType, this.name, applicationContext, -1, tupleFactory, vertex));
            }
        } else if (TapType.FILE == this.tapType) {
            int partitionId = applicationContext.getNodeEngine().getPartitionService().getPartitions()[0].getPartitionId();
            readers.add(HazelcastReaderFactory.getReader(this.tapType, this.name, applicationContext, partitionId, tupleFactory, vertex));
        } else if (TapType.HD_FILE == this.tapType) {
            int partitionId = applicationContext.getNodeEngine().getPartitionService().getPartitions()[0].getPartitionId();
            readers.add(HazelcastReaderFactory.getReader(this.tapType, this.name, applicationContext, partitionId, tupleFactory, vertex));
        } else {
            for (InternalPartition partition : applicationContext.getNodeEngine().getPartitionService().getPartitions()) {
                if (partition.isLocal()) {
                    readers.add(HazelcastReaderFactory.getReader(this.tapType, this.name, applicationContext, partition.getPartitionId(), tupleFactory, vertex));
                }
            }
        }

        return readers.toArray(new TupleReader[readers.size()]);
    }
}
