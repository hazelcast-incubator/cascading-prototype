package com.hazelcast.yarn.impl.container.task.processors.shuffling;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.partition.InternalPartition;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.actor.TupleConsumer;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.container.task.processors.ConsumerTaskProcessor;


public class ShuffledConsumerTaskProcessor extends ConsumerTaskProcessor {
    private final int chunkSize;
    private final boolean receiver;
    private final NodeEngine nodeEngine;
    private final boolean hasActiveConsumers;
    private final TupleWriter[] sendersArray;
    private final boolean hasUnShuffledConsumers;
    private final TupleConsumer[] shuffledConsumers;
    private final Address[] nonPartitionedAddresses;
    private final TupleConsumer[] nonPartitionedWriters;
    private final PartitioningStrategy[] partitionStrategies;
    private final Map<Address, TupleWriter> senders = new HashMap<Address, TupleWriter>();
    private final Map<PartitioningStrategy, Map<Integer, List<TupleConsumer>>> partitionedWriters;

    private int lastConsumedSize = 0;
    private boolean chunkInProgress = false;
    private boolean unShufflersSuccess = false;

    public ShuffledConsumerTaskProcessor(TupleConsumer[] consumers,
                                         TupleContainerProcessor processor,
                                         ContainerContext containerContext,
                                         int taskID) {
        this(consumers, processor, containerContext, taskID, false);
    }

    public ShuffledConsumerTaskProcessor(TupleConsumer[] consumers,
                                         TupleContainerProcessor processor,
                                         ContainerContext containerContext,
                                         int taskID,
                                         boolean receiver) {
        super(filterConsumers(consumers, false), processor, containerContext);
        this.nodeEngine = containerContext.getNodeEngine();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberImpls();
        ApplicationMaster applicationMaster = containerContext.getApplicationContext().getApplicationMaster();

        if (!receiver) {
            for (Member member : members) {
                if (!member.localMember()) {
                    ShufflingSender sender = new ShufflingSender(member, containerContext, taskID);
                    this.senders.put(member.getAddress(), sender);
                    applicationMaster.registerShufflingSender(taskID, containerContext, member, sender);
                }
            }
        }

        this.sendersArray = this.senders.values().toArray(new TupleWriter[this.senders.size()]);
        this.hasUnShuffledConsumers = this.consumers.length > 0;

        if (!this.hasUnShuffledConsumers) {
            this.unShufflersSuccess = true;
        }

        this.shuffledConsumers = filterConsumers(consumers, true);
        this.partitionedWriters = new IdentityHashMap<PartitioningStrategy, Map<Integer, List<TupleConsumer>>>();

        Set<PartitioningStrategy> strategies = new HashSet<PartitioningStrategy>();
        List<InternalPartition> localPartitions = new ArrayList<InternalPartition>();

        for (InternalPartition partition : this.nodeEngine.getPartitionService().getPartitions()) {
            if (partition.isLocal()) {
                localPartitions.add(partition);
            }
        }

        List<TupleConsumer> nonPartitionedConsumers = new ArrayList<TupleConsumer>(this.shuffledConsumers.length);
        Set<Address> nonPartitionedAddresses = new HashSet<Address>(this.shuffledConsumers.length);

        boolean hasActiveConsumers = false;

        // Process writers
        for (TupleConsumer consumer : this.shuffledConsumers) {
            PartitioningStrategy partitionStrategy;
            int partitionId;

            ShufflingStrategy shufflingStrategy = consumer.getShufflingStrategy();

            if (shufflingStrategy != null) {
                Address address = shufflingStrategy.getShufflingAddress(containerContext);

                if (address.equals(this.nodeEngine.getThisAddress())) {
                    hasActiveConsumers = true;
                    nonPartitionedConsumers.add(
                            consumer
                    );
                } else {
                    nonPartitionedAddresses.add(
                            address
                    );
                }

                continue;
            }

            if (consumer instanceof TupleWriter) {
                TupleWriter writer = (TupleWriter) consumer;
                if (!writer.isPartitioned()) {
                    if (writer.getPartitionId() >= 0) {
                        InternalPartition partition = this.nodeEngine.getPartitionService().getPartition(writer.getPartitionId());

                        if (partition.isLocal()) {
                            hasActiveConsumers = true;
                            nonPartitionedConsumers.add(
                                    writer
                            );
                        } else {
                            nonPartitionedAddresses.add(
                                    this.nodeEngine.getPartitionService().getPartitionOwner(partition.getPartitionId())
                            );
                        }
                    } else {
                        nonPartitionedConsumers.add(
                                writer
                        );
                    }
                    continue;
                }

                partitionId = writer.getPartitionId();
                partitionStrategy = writer.getPartitionStrategy();
            } else {
                partitionStrategy = this.nodeEngine.getSerializationService().getGlobalPartitionStrategy();
                partitionId = -1;
            }

            hasActiveConsumers = true;

            strategies.add(partitionStrategy);
            Map<Integer, List<TupleConsumer>> map = this.partitionedWriters.get(partitionStrategy);

            if (map == null) {
                map = new HashMap<Integer, List<TupleConsumer>>();
                this.partitionedWriters.put(partitionStrategy, map);
            }

            if (partitionId >= 0) {
                this.processPartition(map, partitionId).add(consumer);
            } else {
                for (InternalPartition localPartition : localPartitions) {
                    this.processPartition(map, localPartition.getPartitionId()).add(consumer);
                }
            }
        }

        this.receiver = receiver;
        this.hasActiveConsumers = hasActiveConsumers;

        this.partitionStrategies = strategies.toArray(new PartitioningStrategy[strategies.size()]);
        this.nonPartitionedWriters = nonPartitionedConsumers.toArray(new TupleConsumer[nonPartitionedConsumers.size()]);
        this.nonPartitionedAddresses = nonPartitionedAddresses.toArray(new Address[nonPartitionedAddresses.size()]);
        this.chunkSize = nodeEngine.getConfig().getYarnApplicationConfig(containerContext.getApplicationContext().getName()).getTupleChunkSize();
        this.resetState();
    }


    private List<TupleConsumer> processPartition(Map<Integer, List<TupleConsumer>> map, int partitionId) {
        List<TupleConsumer> partitionOwnerWriters = map.get(partitionId);

        if (partitionOwnerWriters == null) {
            partitionOwnerWriters = new ArrayList<TupleConsumer>();
            map.put(partitionId, partitionOwnerWriters);
        }

        return partitionOwnerWriters;
    }

    public void onOpen() {
        super.onOpen();

        for (TupleConsumer tupleConsumer : this.shuffledConsumers) {
            tupleConsumer.open();
        }

        for (TupleWriter tupleWriter : this.sendersArray) {
            tupleWriter.open();
        }

        this.reset();
    }

    public void onClose() {
        super.onClose();

        for (TupleConsumer tupleConsumer : this.shuffledConsumers) {
            tupleConsumer.close();
        }

        for (TupleWriter tupleWriter : this.sendersArray) {
            tupleWriter.close();
        }
    }

    private static TupleConsumer[] filterConsumers(TupleConsumer[] consumers, boolean isShuffled) {
        List<TupleConsumer> filtered = new ArrayList<TupleConsumer>(consumers.length);

        for (TupleConsumer consumer : consumers) {
            if (consumer.isShuffled() == isShuffled)
                filtered.add(consumer);
        }

        return filtered.toArray(new TupleConsumer[filtered.size()]);
    }

    private void resetState() {
        this.lastConsumedSize = 0;
        this.chunkInProgress = false;
        this.unShufflersSuccess = !this.hasUnShuffledConsumers;
    }

    @Override
    public boolean onChunk(TupleInputStream chunk) throws Exception {
        if (chunk.size() > 0) {
            boolean success = false;
            boolean consumed = false;

            /// UnShufflers
            if (!this.receiver) {
                if ((this.hasUnShuffledConsumers) && (!this.unShufflersSuccess)) {
                    this.unShufflersSuccess = super.onChunk(chunk);
                    consumed = super.consumed();
                }
            }

            // Shufflers
            boolean chunkPooled = this.lastConsumedSize >= chunk.size();

            if ((!chunkInProgress) && (!chunkPooled)) {
                this.lastConsumedSize = this.processShufflers(chunk, this.lastConsumedSize);
                chunkPooled = this.lastConsumedSize >= chunk.size();
                this.chunkInProgress = true;
                consumed = true;
            }

            if (this.chunkInProgress) {
                boolean flushed = this.processChunkProgress();
                consumed = true;

                if (flushed) {
                    this.chunkInProgress = false;
                }
            }

            /// Check if we are success with chunk
            if ((!this.chunkInProgress) &&
                    (chunkPooled) &&
                    (this.unShufflersSuccess)) {
                success = true;
                consumed = true;
            }

            if (success) {
                this.resetState();
            }

            this.consumed = consumed;
            return success;
        } else {
            this.consumed = false;
            return true;
        }
    }

    private boolean processChunkProgress() {
        boolean flushed = true;

        for (TupleWriter sender : this.sendersArray) {
            flushed &= sender.isFlushed();
        }

        for (TupleConsumer tupleConsumer : this.shuffledConsumers) {
            flushed &= tupleConsumer.isFlushed();
        }

        return flushed;
    }

    public static final AtomicInteger sctReceiverByChunk = new AtomicInteger(0);
    public static final AtomicInteger sctReceiver = new AtomicInteger(0);
    public static final AtomicInteger sctNonReceiver = new AtomicInteger(0);

    private int processShufflers(TupleInputStream chunk, int lastConsumedSize) throws Exception {
        if (this.partitionStrategies.length > 0) {
            int toIdx = Math.min(lastConsumedSize + this.chunkSize, chunk.size());
            int consumedSize = 0;

            for (int i = lastConsumedSize; i < toIdx; i++) {
                Tuple tuple = chunk.get(i);

                consumedSize++;
                processPartitionStrategies(tuple);
            }

            this.flush();
            return lastConsumedSize + consumedSize;
        } else {
            this.writeToNonPartitionedLocals(chunk);

            if (!this.receiver) {
                this.sendToNonPartitionedRemotes(chunk);
            }

            this.flush();
            return chunk.size();
        }
    }

    private void flush() {
        for (TupleConsumer tupleConsumer : this.shuffledConsumers) {
            tupleConsumer.flush();
        }

        if (!receiver) {
            for (TupleWriter sender : this.sendersArray) {
                sender.flush();
            }
        }
    }

    private void processPartitionStrategies(Tuple tuple) throws Exception {
        PartitioningStrategy tuplePartitioningStrategy = tuple.getPartitioningStrategy();
        int tuplePartitionId = tuple.getPartitionId();

        for (PartitioningStrategy partitionStrategy : this.partitionStrategies) {
            Map<Integer, List<TupleConsumer>> cache = this.partitionedWriters.get(partitionStrategy);
            List<TupleConsumer> writers;

            if ((tuplePartitioningStrategy != null) && (tuplePartitioningStrategy == partitionStrategy) && (tuplePartitionId >= 0)) {
                writers = cache.get(tuplePartitionId);
            } else {
                tuplePartitionId = this.nodeEngine.getPartitionService().getPartitionId(
                        tuple.getKeyData(partitionStrategy, nodeEngine)
                );

                writers = cache.get(tuplePartitionId);
            }

            this.writeToNonPartitionedLocals(tuple);

            if (writers != null) {
                if (receiver) {
                    sctReceiver.incrementAndGet();
                } else {
                    sctNonReceiver.incrementAndGet();
                }
            }

            if (YarnUtil.isEmpty(writers)) {
                Address address = this.nodeEngine.getPartitionService().getPartitionOwner(tuplePartitionId);
                TupleWriter sender = this.senders.get(address);
                sender.consumeTuple(tuple);

                for (Address remoteAddress : this.nonPartitionedAddresses) {
                    if (!remoteAddress.equals(address)) {
                        this.senders.get(remoteAddress).consumeTuple(tuple);
                    }
                }
            } else {
                //Write to locals
                for (int ir = 0; ir < writers.size(); ir++) {
                    TupleConsumer writer = writers.get(ir);
                    writer.consumeTuple(tuple);
                }

                sendToNonPartitionedRemotes(tuple);
            }
        }
    }

    @Override
    public boolean hasActiveConsumers() {
        return hasActiveConsumers;
    }

    private void writeToNonPartitionedLocals(TupleInputStream chunk) throws Exception {
        for (TupleConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeChunk(chunk);
        }
    }

    private void sendToNonPartitionedRemotes(TupleInputStream chunk) throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            this.senders.get(remoteAddress).consumeChunk(chunk);
        }
    }

    private void writeToNonPartitionedLocals(Tuple tuple) throws Exception {
        for (TupleConsumer nonPartitionedWriter : this.nonPartitionedWriters) {
            nonPartitionedWriter.consumeTuple(tuple);
        }
    }

    private void sendToNonPartitionedRemotes(Tuple tuple) throws Exception {
        for (Address remoteAddress : this.nonPartitionedAddresses) {
            this.senders.get(remoteAddress).consumeTuple(tuple);
        }
    }
}