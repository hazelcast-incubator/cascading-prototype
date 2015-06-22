package com.hazelcast.yarn.api.container;

import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;

public interface ContainerStateMachineRequestProcessor<SI extends ContainerEvent> extends StateMachineRequestProcessor<SI> {
    void processRequest(SI event, Object payLoad) throws Exception;
}
