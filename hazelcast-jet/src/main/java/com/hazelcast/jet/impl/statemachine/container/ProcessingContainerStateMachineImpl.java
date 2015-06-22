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

package com.hazelcast.jet.impl.statemachine.container;

import java.util.Map;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.LinkedMapBuilder;
import com.hazelcast.jet.api.executor.ApplicationExecutor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.ProcessingContainerStateMachine;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.DataContainerEvent;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerState;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerResponse;

public class ProcessingContainerStateMachineImpl extends
        AbstractStateMachineImpl<DataContainerEvent, ProcessingContainerState, ProcessingContainerResponse>
        implements ProcessingContainerStateMachine {
    private static final Map<ProcessingContainerState, Map<DataContainerEvent, ProcessingContainerState>>
            STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<ProcessingContainerState, Map<DataContainerEvent, ProcessingContainerState>>builder().
                    put(
                            ProcessingContainerState.NEW, LinkedMapBuilder.of(
                                    DataContainerEvent.START, ProcessingContainerState.AWAITING,
                                    DataContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED
                            )
                    ).
                    put(
                            ProcessingContainerState.AWAITING, LinkedMapBuilder.of(
                                    DataContainerEvent.EXECUTE, ProcessingContainerState.EXECUTION,
                                    DataContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED
                            )
                    ).
                    put(
                            ProcessingContainerState.EXECUTION, LinkedMapBuilder.of(
                                    DataContainerEvent.EXECUTION_COMPLETED, ProcessingContainerState.AWAITING,
                                    DataContainerEvent.INTERRUPT, ProcessingContainerState.INTERRUPTING
                            )
                    ).
                    put(
                            ProcessingContainerState.INTERRUPTING, LinkedMapBuilder.of(
                                    DataContainerEvent.EXECUTION_COMPLETED, ProcessingContainerState.AWAITING,
                                    DataContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED,
                                    DataContainerEvent.INTERRUPTED, ProcessingContainerState.AWAITING,
                                    DataContainerEvent.INVALIDATE_CONTAINER, ProcessingContainerState.INVALIDATED
                            )
                    ).
                    put(
                            ProcessingContainerState.INVALIDATED, LinkedMapBuilder.of(
                                    DataContainerEvent.INVALIDATE_CONTAINER, ProcessingContainerState.INVALIDATED,
                                    DataContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED
                            )
                    ).
                    build();

    public ProcessingContainerStateMachineImpl(String name,
                                               StateMachineRequestProcessor<DataContainerEvent> processor,
                                               NodeEngine nodeEngine,
                                               ApplicationContext applicationContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, nodeEngine, applicationContext);
    }

    @Override
    protected ApplicationExecutor getExecutor() {
        return getApplicationContext().getExecutorContext().getDataContainerStateMachineExecutor();
    }

    @Override
    protected ProcessingContainerState defaultState() {
        return ProcessingContainerState.NEW;
    }

    @Override
    protected ProcessingContainerResponse output(DataContainerEvent dataContainerEvent, ProcessingContainerState nextState) {
        if (nextState == null) {
            return ProcessingContainerResponse.FAILURE;
        } else {
            return ProcessingContainerResponse.SUCCESS;
        }
    }
}
