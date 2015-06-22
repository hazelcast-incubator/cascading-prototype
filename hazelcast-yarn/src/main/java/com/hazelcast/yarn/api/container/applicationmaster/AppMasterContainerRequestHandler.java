package com.hazelcast.yarn.api.container.applicationmaster;

import java.util.concurrent.Future;

import com.hazelcast.yarn.api.container.ContainerRequestHandler;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;

public interface AppMasterContainerRequestHandler extends ContainerRequestHandler<ApplicationMasterEvent, ApplicationMasterResponse> {
    <P> Future<ApplicationMasterResponse> handleContainerRequest(ContainerRequest<ApplicationMasterEvent, P> event);
}
