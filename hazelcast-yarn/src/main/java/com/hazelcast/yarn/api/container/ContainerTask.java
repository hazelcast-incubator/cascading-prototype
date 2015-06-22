package com.hazelcast.yarn.api.container;

import java.util.List;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.yarn.api.dag.Edge;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.executor.Task;
import com.hazelcast.yarn.api.actor.TupleActor;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.actor.TupleProducer;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;


public interface ContainerTask extends Task {
    void start(List<? extends TupleProducer> producers);

    void interrupt();

    void markInvalidated();

    void destroy();

    void beforeExecution();

    void registerSinkWriters(List<TupleWriter> sinkWriters);

    TupleActor registerOutputChannel(TupleChannel channel, Edge edge);

    void handleProducerCompleted(TupleProducer actor);

    void handleShufflingReceiverCompleted(TupleProducer actor);

    void handleShufflingReceiverFinalizing(TupleProducer actor);

    void registerShufflingReceiver(Member member, ShufflingReceiver receiver);

    void registerShufflingSender(Member member, ShufflingSender sender);

    ShufflingReceiver getShufflingReceiver(Address endPoint);

    ShufflingSender getShufflingSender(Address endPoint);

    Vertex getVertex();

    void startFinalization();
}
