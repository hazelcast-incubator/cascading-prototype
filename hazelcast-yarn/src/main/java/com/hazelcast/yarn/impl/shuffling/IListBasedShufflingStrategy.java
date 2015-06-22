package com.hazelcast.yarn.impl.shuffling;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class IListBasedShufflingStrategy implements ShufflingStrategy {
    private final String listName;

    public IListBasedShufflingStrategy(String listName) {
        this.listName = listName;
    }

    @Override
    public Address getShufflingAddress(ContainerContext containerContext) {
        NodeEngine nodeEngine = containerContext.getNodeEngine();

        int partitionId = nodeEngine.getPartitionService().getPartitionId(
                nodeEngine.getSerializationService().toData(
                        this.listName,
                        StringPartitioningStrategy.INSTANCE
                )
        );

        return nodeEngine.getPartitionService().getPartitionOwner(partitionId);
    }
}
