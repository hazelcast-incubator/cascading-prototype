package com.hazelcast.yarn.impl.tap.sink;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;


public class HazelcastListPartitionWriter extends AbstractHazelcastWriter {
    private final ListContainer listContainer;

    private static int getPartitionId(String name, NodeEngine nodeEngine) {
        return nodeEngine.getPartitionService().getPartitionId(nodeEngine.getSerializationService().toData(name, StringPartitioningStrategy.INSTANCE));
    }

    public HazelcastListPartitionWriter(ContainerContext containerContext, SinkTapWriteStrategy sinkTapWriteStrategy, String name) {
        super(containerContext, getPartitionId(name, containerContext.getApplicationContext().getNodeEngine()), sinkTapWriteStrategy);
        ListService service = ((NodeEngineImpl) containerContext.getApplicationContext().getNodeEngine()).getService(ListService.SERVICE_NAME);
        this.listContainer = service.getOrCreateContainer(name, false);
    }

    @Override
    protected void processChunk(TupleInputStream chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            Tuple tuple = chunk.get(i);

            if (tuple == null) {
                continue;
            }

            if (!this.listContainer.hasEnoughCapacity(chunk.size())) {
                throw new IllegalStateException("IList " + this.getName() + " capacity exceeded");
            }

            if (!(tuple.getKey(0) instanceof Integer)) {
                throw new IllegalStateException("Key for IList tuple should be Integer");
            }

            this.listContainer.add(tuple.getValueData(getPartitionStrategy(), getNodeEngine()));
        }
    }

    @Override
    protected void onOpen() {
        if (getSinkTapWriteStrategy() == SinkTapWriteStrategy.CLEAR_AND_REPLACE) {
            this.listContainer.clear();
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}
