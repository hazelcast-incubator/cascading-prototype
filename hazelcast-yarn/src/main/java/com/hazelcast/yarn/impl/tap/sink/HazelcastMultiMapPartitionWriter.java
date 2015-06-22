package com.hazelcast.yarn.impl.tap.sink;

import java.util.Collection;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class HazelcastMultiMapPartitionWriter extends AbstractHazelcastWriter {
    private final MultiMapContainer container;

    public HazelcastMultiMapPartitionWriter(ContainerContext containerContext, int partitionId, SinkTapWriteStrategy sinkTapWriteStrategy, String name) {
        super(containerContext, partitionId, sinkTapWriteStrategy);
        MultiMapService service = ((NodeEngineImpl) containerContext.getApplicationContext().getNodeEngine()).getService(MultiMapService.SERVICE_NAME);
        this.container = service.getOrCreateCollectionContainer(getPartitionId(), name);
    }

    @Override
    protected void processChunk(TupleInputStream chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            Tuple tuple = chunk.get(i);
            Data dataKey = tuple.getKeyData(getNodeEngine());
            Collection<MultiMapRecord> coll = this.container.getMultiMapValueOrNull(dataKey).getCollection(false);
            long recordId = this.container.nextId();

            for (int idx = 0; idx < tuple.valueSize(); idx++) {
                Data dataValue = getNodeEngine().getSerializationService().toData(tuple.getValue(idx));
                MultiMapRecord record = new MultiMapRecord(recordId, dataValue);
                coll.add(record);
            }
        }
    }

    @Override
    protected void onOpen() {
        this.container.clear();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
