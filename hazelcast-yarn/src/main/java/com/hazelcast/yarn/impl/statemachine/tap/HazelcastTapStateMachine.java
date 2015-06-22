package com.hazelcast.yarn.impl.statemachine.tap;

import java.util.Map;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.LinkedMapBuilder;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.api.tap.TapEvent;
import com.hazelcast.yarn.api.tap.TapState;
import com.hazelcast.yarn.api.tap.TapResponse;
import com.hazelcast.yarn.impl.statemachine.AbstractStateMachineImpl;

public class HazelcastTapStateMachine extends AbstractStateMachineImpl<TapEvent, TapState, TapResponse> {
    private static final Map<TapState, Map<TapEvent, TapState>> stateTransitionMatrix = LinkedMapBuilder.<TapState, Map<TapEvent, TapState>>builder()
            .put(
                    TapState.OPENED, LinkedMapBuilder.of(
                            TapEvent.CLOSE, TapState.CLOSED
                    )
            )
            .put(
                    TapState.CLOSED, LinkedMapBuilder.of(
                            TapEvent.OPEN, TapState.OPENED,
                            TapEvent.CLOSE, TapState.CLOSED
                    )
            ).build();

    public HazelcastTapStateMachine(String name, NodeEngine nodeEngine,ApplicationContext applicationContext) {
        super(name, stateTransitionMatrix, null, nodeEngine,applicationContext);
    }

    @Override
    protected ApplicationExecutor getExecutor() {
        return getApplicationContext().getTapStateMachineExecutor();
    }

    @Override
    protected TapState defaultState() {
        return TapState.CLOSED;
    }

    @Override
    protected TapResponse output(TapEvent tapEvent, TapState nextState) {
        if (nextState == null)
            return TapResponse.FAILURE;
        else
            return TapResponse.SUCCESS;
    }
}
