package com.hazelcast.yarn.impl.statemachine.container;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.LinkedMapBuilder;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.impl.container.RequestPayLoad;
import com.hazelcast.yarn.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.TupleContainerStateMachine;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerState;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerResponse;

public class TupleContainerStateMachineImpl extends AbstractStateMachineImpl<TupleContainerEvent, TupleContainerState, TupleContainerResponse>
        implements TupleContainerStateMachine {
    private static final Map<TupleContainerState, Map<TupleContainerEvent, TupleContainerState>> stateTransitionMatrix =
            LinkedMapBuilder.<TupleContainerState, Map<TupleContainerEvent, TupleContainerState>>builder().
                    put(
                            TupleContainerState.NEW, LinkedMapBuilder.of(
                                    TupleContainerEvent.START, TupleContainerState.AWAITING,
                                    TupleContainerEvent.FINALIZE, TupleContainerState.FINALIZED
                            )
                    ).
                    put(
                            TupleContainerState.AWAITING, LinkedMapBuilder.of(
                                    TupleContainerEvent.EXECUTE, TupleContainerState.EXECUTION,
                                    TupleContainerEvent.FINALIZE, TupleContainerState.FINALIZED
                            )
                    ).
                    put(
                            TupleContainerState.EXECUTION, LinkedMapBuilder.of(
                                    TupleContainerEvent.EXECUTION_COMPLETED, TupleContainerState.AWAITING,
                                    TupleContainerEvent.INTERRUPT, TupleContainerState.INTERRUPTING
                            )
                    ).
                    put(
                            TupleContainerState.INTERRUPTING, LinkedMapBuilder.of(
                                    TupleContainerEvent.EXECUTION_COMPLETED, TupleContainerState.AWAITING,
                                    TupleContainerEvent.FINALIZE, TupleContainerState.FINALIZED,
                                    TupleContainerEvent.INTERRUPTED, TupleContainerState.AWAITING,
                                    TupleContainerEvent.INVALIDATE_CONTAINER, TupleContainerState.INVALIDATED
                            )
                    ).
                    put(
                            TupleContainerState.INVALIDATED, LinkedMapBuilder.of(
                                    TupleContainerEvent.INVALIDATE_CONTAINER, TupleContainerState.INVALIDATED,
                                    TupleContainerEvent.FINALIZE, TupleContainerState.FINALIZED
                            )
                    ).
                    build();

    public TupleContainerStateMachineImpl(String name,
                                          StateMachineRequestProcessor<TupleContainerEvent> processor,
                                          NodeEngine nodeEngine,
                                          ApplicationContext applicationContext) {
        super(name, stateTransitionMatrix, processor, nodeEngine, applicationContext);
    }

    @Override
    protected ApplicationExecutor getExecutor() {
        return getApplicationContext().getTupleContainerStateMachineExecutor();
    }

    @Override
    protected TupleContainerState defaultState() {
        return TupleContainerState.NEW;
    }

    @Override
    protected TupleContainerResponse output(TupleContainerEvent tupleContainerEvent, TupleContainerState nextState) {
        if (nextState == null)
            return TupleContainerResponse.FAILURE;
        else
            return TupleContainerResponse.SUCCESS;
    }
}
