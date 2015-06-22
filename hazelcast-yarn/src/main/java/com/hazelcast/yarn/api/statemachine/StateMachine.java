package com.hazelcast.yarn.api.statemachine;

import java.util.concurrent.Future;

public interface StateMachine<Input extends StateMachineEvent, State extends StateMachineState, Output extends StateMachineOutput>  {
    State currentState();

    Output getOutput();

    <P> Future<Output> handleRequest(StateMachineRequest<Input, P> request);
}
