package com.hazelcast.yarn.api.container.applicationmaster;

import java.util.List;

import com.hazelcast.core.Member;
import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.yarn.api.container.Container;
import com.hazelcast.yarn.impl.hazelcast.YarnPacket;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.ExecutionErrorHolder;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;


public interface ApplicationMaster extends Container<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {
    void handleContainerInterrupted(TupleContainer tapContainer);

    void handleContainerCompleted(TupleContainer tapContainer);

    void registerExecution();

    void registerInterruption();

    void invalidateApplication(Object reason);

    BlockingQueue<Object> getExecutionMailBox();

    BlockingQueue<Object> getInterruptionMailBox();

    void setExecutionError(ExecutionErrorHolder executionError);

    TupleContainer getContainerByVertex(Vertex vertex);

    void registerContainer(Vertex vertex, TupleContainer container);

    List<TupleContainer> containers();

    int receiveYarnPacket(YarnPacket yarnPacket);

    void registerShufflingReceiver(int taskID, ContainerContext containerContext, Member member, ShufflingReceiver receiver);

    void registerShufflingSender(int taskID, ContainerContext containerContext, Member member, ShufflingSender sender);

    BlockingQueue<Object> synchronizeWithOtherNodes(TupleContainer container);

    void setDag(DAG dag);

    DAG getDag();

    void addToExecutionMailBox(Object object);
}
