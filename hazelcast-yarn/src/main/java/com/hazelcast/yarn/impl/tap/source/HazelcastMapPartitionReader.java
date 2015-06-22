package com.hazelcast.yarn.impl.tap.source;


import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.impl.tuple.TupleIterator;
import com.hazelcast.yarn.api.tuple.TupleConvertor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastMapPartitionReader<K, V> extends AbstractHazelcastReader<K, V> {
    private final MapConfig mapConfig;
    private final PartitioningStrategy partitioningStrategy;

    public static final AtomicInteger read1 = new AtomicInteger(0);
    public static final AtomicInteger read2 = new AtomicInteger(0);

    private final TupleConvertor<Record, K, V> TUPLE_CONVERTER = new TupleConvertor<Record, K, V>() {
        @Override
        public Tuple<K, V> convert(Record record, SerializationService ss) {
            Object value;

            if (mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY) {
                value = ss.toObject(record.getValue());
            } else {
                value = record.getValue();
            }

            return getTupleFactory().tuple(
                    ss.<K>toObject(record.getKey()),
                    (V) value,
                    getPartitionId(),
                    partitioningStrategy
            );
        }
    };

    public HazelcastMapPartitionReader(ApplicationContext applicationContext, String name, int partitionId, TupleFactory tupleFactory, Vertex vertex) {
        super(applicationContext, name, partitionId, tupleFactory, vertex);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) applicationContext.getNodeEngine();

        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();

        if (partitioningStrategy == null) {
            this.partitioningStrategy = nodeEngine.getSerializationService().getGlobalPartitionStrategy();
        } else {
            this.partitioningStrategy = partitioningStrategy;
        }
    }

    @Override
    protected void onClose() {

    }

    @Override
    public void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) getNodeEngine();
        SerializationService ss = nei.getSerializationService();
        MapService mapService = nei.getService(MapService.SERVICE_NAME);
        PartitionContainer partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        RecordStore recordStore = partitionContainer.getRecordStore(getName());
        this.iterator = new TupleIterator<Record, K, V>(recordStore.iterator(), TUPLE_CONVERTER, ss);
    }
}
