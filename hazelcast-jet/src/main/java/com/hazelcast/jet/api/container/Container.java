/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.List;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.statemachine.StateMachine;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.container.ContainerEvent;
import com.hazelcast.jet.api.statemachine.container.ContainerState;

public interface Container
        <SI extends ContainerEvent,
                SS extends ContainerState,
                SO extends ContainerResponse> extends
        ContainerRequestHandler<SI, SO>,
        ContainerStateMachineRequestProcessor<SI> {
    NodeEngine getNodeEngine();

    StateMachine<SI, SS, SO> getStateMachine();

    ApplicationContext getApplicationContext();

    List<ProcessingContainer> getFollowers();

    List<ProcessingContainer> getPredecessors();

    void addFollower(ProcessingContainer container);

    void addPredecessor(ProcessingContainer container);

    ContainerContext getContainerContext();

    int getID();
}
