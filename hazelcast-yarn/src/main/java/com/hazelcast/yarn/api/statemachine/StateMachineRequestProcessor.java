package com.hazelcast.yarn.api.statemachine;

public interface StateMachineRequestProcessor<SI extends StateMachineEvent> {
    void processRequest(SI event, Object payLoad) throws Exception;
}
