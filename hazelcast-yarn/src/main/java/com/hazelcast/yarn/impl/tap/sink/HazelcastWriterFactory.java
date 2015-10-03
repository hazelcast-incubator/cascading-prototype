package com.hazelcast.yarn.impl.tap.sink;


import com.hazelcast.yarn.api.tap.TapType;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.container.ContainerContext;

public class HazelcastWriterFactory {
    public static TupleWriter getWriter(TapType tapType,
                                        String name,
                                        SinkTapWriteStrategy sinkTapWriteStrategy,
                                        ContainerContext containerContext,
                                        int partitionId) {
        switch (tapType) {
            case HAZELCAST_LIST:
                return new HazelcastListPartitionWriter(containerContext, sinkTapWriteStrategy, name);
            case HAZELCAST_MAP:
                return new HazelcastMapPartitionWriter(containerContext, partitionId, sinkTapWriteStrategy, name);
            case HAZELCAST_MULTIMAP:
                return new HazelcastMultiMapPartitionWriter(containerContext, partitionId, sinkTapWriteStrategy, name);
            case FILE:
                return new TupleFileWriter(containerContext, FileWrapper.getWrapper(name), partitionId);
            case HD_FILE:
                return new HDTupleFileWriter(containerContext, HDFileManager.getWrapper(name));
            default:
                throw new IllegalStateException("Unknown tuple type: " + tapType);
        }
    }
}