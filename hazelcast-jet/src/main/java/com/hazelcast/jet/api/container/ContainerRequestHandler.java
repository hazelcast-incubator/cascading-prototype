/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.api.container;

import java.util.concurrent.Future;

import com.hazelcast.jet.api.statemachine.container.ContainerEvent;
import com.hazelcast.jet.api.statemachine.container.ContainerRequest;

/**
 * Interface container's state-machine event handler;
 *
 * @param <E> - type of the input container state-machine event;
 * @param <R> - type of the output container state-machine event;
 */
public interface ContainerRequestHandler<E extends ContainerEvent, R extends ContainerResponse> {
    /**
     * Handle's container's request with state-machine's input event;
     *
     * @param event - corresponding request;
     * @param <P>   - type of request payload;
     * @return - awaiting future;
     */
    <P> Future<R> handleContainerRequest(ContainerRequest<E, P> event);
}
