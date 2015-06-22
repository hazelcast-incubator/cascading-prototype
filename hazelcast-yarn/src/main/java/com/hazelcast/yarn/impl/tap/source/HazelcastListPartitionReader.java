package com.hazelcast.yarn.impl.tap.source;

import java.util.List;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.tuple.TupleConvertor;
import com.hazelcast.yarn.impl.tuple.TupleIterator;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class HazelcastListPartitionReader<K, V> extends AbstractHazelcastReader<K, V> {
    private final TupleConvertor<CollectionItem, K, V> TUPLE_CONVERTER = new TupleConvertor<CollectionItem, K, V>() {
        @Override
        public Tuple<K, V> convert(CollectionItem item, SerializationService ss) {
            return getTupleFactory().tuple(
                    (K[]) new Object[]{item.getItemId()},
                    (V[]) new Object[]{ss.toObject(item.getValue())},
                    getPartitionId(),
                    StringPartitioningStrategy.INSTANCE
            );
        }
    };

    public HazelcastListPartitionReader(ApplicationContext applicationContext, String name, TupleFactory tupleFactory, Vertex vertex) {
        super(applicationContext, name, getPartitionId(applicationContext.getNodeEngine(), name), tupleFactory, vertex);
    }

    public static int getPartitionId(NodeEngine nodeEngine, String name) {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        SerializationService ss = nei.getSerializationService();
        InternalPartitionService ps = nei.getPartitionService();
        Data data = ss.toData(name, StringPartitioningStrategy.INSTANCE);
        return ps.getPartitionId(data);
    }

    @Override
    protected void onClose() {

    }

    protected void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) getNodeEngine();
        ListService listService = nei.getService(ListService.SERVICE_NAME);
        ListContainer listContainer = listService.getOrCreateContainer(getName(), false);
        List<CollectionItem> items = listContainer.getCollection();
        SerializationService ss = nei.getSerializationService();
        this.iterator = new TupleIterator<CollectionItem, K, V>(items.iterator(), TUPLE_CONVERTER, ss);
    }
}
