package com.hazelcast.yarn.api.statemachine.application;

import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;

public interface ApplicationStateMachineRequestProcessor extends StateMachineRequestProcessor<ApplicationEvent> {
    void processRequest(ApplicationEvent event, Object payLoad) throws Exception;
}
