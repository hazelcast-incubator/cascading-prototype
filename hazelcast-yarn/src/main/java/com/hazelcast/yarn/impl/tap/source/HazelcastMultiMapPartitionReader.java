package com.hazelcast.yarn.impl.tap.source;

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.yarn.api.tuple.TupleConvertor;
import com.hazelcast.yarn.impl.tuple.TupleIterator;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;


public class HazelcastMultiMapPartitionReader<K, V> extends AbstractHazelcastReader<K, V> {
    private final TupleConvertor<Map.Entry<Data, MultiMapValue>, K, V> TUPLE_CONVERTER = new TupleConvertor<Map.Entry<Data, MultiMapValue>, K, V>() {
        @Override
        public Tuple<K, V> convert(final Map.Entry<Data, MultiMapValue> entry, SerializationService ss) {
            K key = ss.toObject(entry.getKey());
            Collection<MultiMapRecord> multiMapRecordList = entry.getValue().getCollection(false);
            List<MultiMapRecord> list;

            if (multiMapRecordList instanceof List) {
                list = (List<MultiMapRecord>) multiMapRecordList;
            } else {
                throw new IllegalStateException("Only list multimap is supported");
            }

            Object[] values = new Object[]{multiMapRecordList.size()};

            for (int idx = 0; idx < list.size(); idx++) {
                values[idx] = list.get(0).getObject();
            }

            return getTupleFactory().tuple(
                    (K[]) new Object[]{key},
                    (V[]) values,
                    getPartitionId(),
                    StringAndPartitionAwarePartitioningStrategy.INSTANCE
            );
        }
    };

    public HazelcastMultiMapPartitionReader(ApplicationContext applicationContext,
                                            String name,
                                            int partitionId,
                                            TupleFactory tupleFactory,
                                            Vertex vertex) {
        super(applicationContext, name, partitionId, tupleFactory, vertex);
    }

    @Override
    protected void onClose() {

    }

    @Override
    public void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) getNodeEngine();
        SerializationService ss = nei.getSerializationService();
        MultiMapService multiMapService = nei.getService(MultiMapService.SERVICE_NAME);
        MultiMapContainer multiMapContainer = multiMapService.getPartitionContainer(getPartitionId()).getCollectionContainer(getName());
        Iterator<Map.Entry<Data, MultiMapValue>> it = multiMapContainer.getMultiMapValues().entrySet().iterator();
        this.iterator = new TupleIterator<Map.Entry<Data, MultiMapValue>, K, V>(it, TUPLE_CONVERTER, ss);
    }
}
