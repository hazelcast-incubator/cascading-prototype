package com.hazelcast.yarn.api.statemachine;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.application.ApplicationContext;

public interface StateMachineFactory<Input extends StateMachineEvent, State extends StateMachineState, Output extends StateMachineOutput> {
    StateMachine<Input, State, Output> newStateMachine(String name, StateMachineRequestProcessor<Input> processor, NodeEngine nodeEngine, ApplicationContext applicationContext);
}
