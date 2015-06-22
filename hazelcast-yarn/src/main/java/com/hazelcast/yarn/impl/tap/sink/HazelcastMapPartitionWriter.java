package com.hazelcast.yarn.impl.tap.sink;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;

public class HazelcastMapPartitionWriter extends AbstractHazelcastWriter {
    private final MapConfig mapConfig;
    private final RecordStore recordStore;
    private final PartitioningStrategy partitioningStrategy;

    public HazelcastMapPartitionWriter(ContainerContext containerContext, int partitionId, SinkTapWriteStrategy sinkTapWriteStrategy, String name) {
        super(containerContext, partitionId, sinkTapWriteStrategy);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getApplicationContext().getNodeEngine();
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        this.recordStore = mapServiceContext.getPartitionContainer(getPartitionId()).getRecordStore(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();

        if (partitioningStrategy == null) {
            partitioningStrategy = getNodeEngine().getSerializationService().getGlobalPartitionStrategy();
        }

        this.partitioningStrategy = partitioningStrategy;
    }

    @Override
    protected void processChunk(TupleInputStream chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            Tuple tuple = chunk.get(i);

            Object dataKey;
            Object dataValue;

            dataKey = tuple.getKeyData(this.partitioningStrategy, getNodeEngine());

            if (mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY) {
                dataValue = tuple.getValueData(this.partitioningStrategy, getNodeEngine());
            } else {
                dataValue = tuple.valueSize() == 1 ? tuple.getValue(0) : tuple.cloneValues();
            }

            this.recordStore.put((Data) dataKey, dataValue, -1);
        }
    }

    @Override
    protected void onOpen() {
        this.recordStore.clear();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.partitioningStrategy;
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
