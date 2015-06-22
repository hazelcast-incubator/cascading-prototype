package com.hazelcast.yarn.impl.tap.source;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tap.TapType;
import com.hazelcast.yarn.api.tuple.TupleReader;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class HazelcastReaderFactory {
    public static <K, V> TupleReader getReader(TapType tapType,
                                               String name,
                                               ApplicationContext applicationContext,
                                               int partitionId,
                                               TupleFactory tupleFactory,
                                               Vertex vertex) {
        switch (tapType) {
            case HAZELCAST_LIST:
                return new HazelcastListPartitionReader<K, V>(applicationContext, name, tupleFactory, vertex);
            case HAZELCAST_MAP:
                return new HazelcastMapPartitionReader<K, V>(applicationContext, name, partitionId, tupleFactory, vertex);
            case HAZELCAST_MULTIMAP:
                return new HazelcastMultiMapPartitionReader<K, V>(applicationContext, name, partitionId, tupleFactory, vertex);
            case FILE:
                return new TupleFileReader(applicationContext, vertex, tupleFactory, name);
            case HD_FILE:
                return new HDTupleFileReader(applicationContext, vertex, tupleFactory, name);
            default:
                throw new IllegalStateException("Unknown tuple type: " + tapType);
        }
    }
}
