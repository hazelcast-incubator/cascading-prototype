package com.hazelcast.yarn.impl.container.task.processors.shuffling;

import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import com.hazelcast.core.Member;
import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.TupleReader;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.impl.tuple.io.DefaultTupleIOStream;
import com.hazelcast.yarn.api.container.task.TaskProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.container.task.processors.ActorTaskProcessor;

public class ShuffledActorTaskProcessor extends ActorTaskProcessor {
    private int nextReceiverIdx = 0;

    private final TupleProducer[] receivers;
    private final DefaultTupleIOStream receivedTupleStream;
    private final TaskProcessor receiverConsumerProcessor;

    private final boolean hasActiveProducers;

    public ShuffledActorTaskProcessor(TupleProducer[] producers,
                                      TupleContainerProcessor processor,
                                      ContainerContext containerContext,
                                      TaskProcessor senderConsumerProcessor,
                                      TaskProcessor receiverConsumerProcessor,
                                      int taskID) {
        super(producers, processor, containerContext, senderConsumerProcessor, taskID);

        this.receiverConsumerProcessor = receiverConsumerProcessor;

        Collection<MemberImpl> members = containerContext.getNodeEngine().getClusterService().getMemberImpls();
        List<TupleProducer> receivers = new ArrayList<TupleProducer>();
        ApplicationMaster applicationMaster = containerContext.getApplicationContext().getApplicationMaster();

        boolean hasActiveChannels = false;

        //Check if we will receive tuples from input channels
        for (TupleProducer producer : producers) {
            if (producer instanceof TupleReader) {
                hasActiveChannels = true;
            }
        }

        for (Edge edge : containerContext.getVertex().getInputEdges()) {
            if (edge.getShufflingStrategy() == null) {
                hasActiveChannels = true;
            } else {
                if (edge.getShufflingStrategy().getShufflingAddress(containerContext).equals(containerContext.getNodeEngine().getThisAddress())) {
                    hasActiveChannels = true;
                }
            }
        }

        this.hasActiveProducers = hasActiveChannels;

        for (Member member : members) {
            if (!member.localMember()) {
                //Registration to the AppMaster
                ShufflingReceiver receiver = new ShufflingReceiver(containerContext);
                applicationMaster.registerShufflingReceiver(taskID, containerContext, member, receiver);
                receivers.add(receiver);
            }
        }

        int tupleChunkSize = containerContext.getApplicationContext().getYarnApplicationConfig().getTupleChunkSize();
        this.receivedTupleStream = new DefaultTupleIOStream(new Tuple[tupleChunkSize]);
        this.receivers = receivers.toArray(new TupleProducer[receivers.size()]);
    }

    @Override
    public void onOpen() {
        super.onOpen();

        for (TupleProducer receiver : this.receivers) {
            receiver.open();
        }
    }

    @Override
    public boolean process() throws Exception {
        if (this.receivedTupleStream.size() > 0) {
            boolean success = this.receiverConsumerProcessor.onChunk(this.receivedTupleStream);

            if (success) {
                this.receivedTupleStream.reset();
            }

            return success;
        } else if (this.tupleOutputStream.size() > 0) {
            return super.process();
        } else {
            boolean success;

            if (this.receivers.length > 0) {
                success = this.processReceivers();

                if (success) {
                    this.receivedTupleStream.reset();
                }
            } else {
                success = true;
            }

            success = success && super.process();
            return success;
        }
    }

    @Override
    public boolean hasActiveProducers() {
        return this.hasActiveProducers;
    }

    public static final AtomicInteger fff = new AtomicInteger(0);

    private boolean processReceivers() throws Exception {
        int lastIdx = 0;

        for (int i = this.nextReceiverIdx; i < this.receivers.length; i++) {
            lastIdx = i;

            TupleProducer receiver = this.receivers[i];
            Tuple[] outChunk = receiver.produce();

            if (!YarnUtil.isEmpty(outChunk)) {
                this.produced = true;
                this.receivedTupleStream.consumeChunk(outChunk, receiver.lastProducedCount());

                fff.addAndGet(this.receivedTupleStream.size());

                if (!this.receiverConsumerProcessor.onChunk(this.receivedTupleStream)) {
                    this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;
                    return false;
                }
            }
        }

        this.nextReceiverIdx = (lastIdx + 1) % this.receivers.length;

        return true;
    }
}
