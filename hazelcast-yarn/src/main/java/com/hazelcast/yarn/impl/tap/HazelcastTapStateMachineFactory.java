package com.hazelcast.yarn.impl.tap;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tap.TapState;
import com.hazelcast.yarn.api.tap.TapEvent;
import com.hazelcast.yarn.api.tap.TapResponse;
import com.hazelcast.yarn.api.statemachine.StateMachine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.impl.statemachine.tap.HazelcastTapStateMachine;

public class HazelcastTapStateMachineFactory implements StateMachineFactory<TapEvent, TapState, TapResponse> {
    @Override
    public StateMachine newStateMachine(String name, StateMachineRequestProcessor processor, NodeEngine nodeEngine, ApplicationContext applicationContext) {
        return new HazelcastTapStateMachine(name, nodeEngine, applicationContext);
    }
}
