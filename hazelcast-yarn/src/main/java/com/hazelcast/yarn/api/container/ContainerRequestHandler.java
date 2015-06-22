package com.hazelcast.yarn.api.container;

import java.util.concurrent.Future;

import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;

public interface ContainerRequestHandler<E extends ContainerEvent, R extends ContainerResponse> {
    <P> Future<R> handleContainerRequest(ContainerRequest<E, P> event);
}
