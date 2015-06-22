package com.hazelcast.yarn.impl.tap.sink;

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tap.SinkTap;
import com.hazelcast.yarn.api.tap.TapType;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.impl.actor.shuffling.ShufflingWriter;
import com.hazelcast.yarn.impl.tap.source.HazelcastListPartitionReader;

public class HazelcastSinkTap extends SinkTap {
    private static final SinkTapWriteStrategy DEFAULT_TAP_STRATEGY = SinkTapWriteStrategy.CLEAR_AND_REPLACE;

    private final String name;
    private final TapType tapType;
    private final SinkTapWriteStrategy sinkTapWriteStrategy;

    public HazelcastSinkTap(String name, TapType tapType) {
        this(name, tapType, DEFAULT_TAP_STRATEGY);
    }

    public HazelcastSinkTap(String name, TapType tapType, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.name = name;
        this.tapType = tapType;
        this.sinkTapWriteStrategy = sinkTapWriteStrategy;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TapType getType() {
        return this.tapType;
    }

    @Override
    public TupleWriter[] getWriters(NodeEngine nodeEngine, ContainerContext containerContext) {
        List<TupleWriter> writers = new ArrayList<TupleWriter>();

        if (TapType.HAZELCAST_LIST == tapType) {
            int partitionId = HazelcastListPartitionReader.getPartitionId(nodeEngine, this.name);
            InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);
            int realPartitionId = ((partition != null) && (partition.isLocal())) ? partitionId : -1;
            writers.add(
                    new ShufflingWriter(
                            HazelcastWriterFactory.getWriter(this.tapType, name, getTapStrategy(), containerContext, realPartitionId),
                            nodeEngine,
                            containerContext
                    )
            );
        } else if (TapType.FILE == tapType) {
            int partitionId = nodeEngine.getPartitionService().getPartitions()[0].getPartitionId();

            writers.add(
                    HazelcastWriterFactory.getWriter(this.tapType, this.name, getTapStrategy(), containerContext, partitionId)
            );
        } else if (TapType.HD_FILE == tapType) {
            int partitionId = nodeEngine.getPartitionService().getPartitions()[0].getPartitionId();

            writers.add(
                    HazelcastWriterFactory.getWriter(this.tapType, this.name, getTapStrategy(), containerContext, partitionId)
            );
        } else {
            for (InternalPartition partition : nodeEngine.getPartitionService().getPartitions()) {
                if (partition.isLocal()) {
                    writers.add(
                            new ShufflingWriter(
                                    HazelcastWriterFactory.getWriter(
                                            this.tapType,
                                            this.name,
                                            this.getTapStrategy(),
                                            containerContext,
                                            partition.getPartitionId()
                                    ),
                                    nodeEngine,
                                    containerContext
                            )
                    );
                }
            }
        }

        return writers.toArray(new TupleWriter[writers.size()]);
    }

    @Override
    public SinkTapWriteStrategy getTapStrategy() {
        return sinkTapWriteStrategy;
    }
}