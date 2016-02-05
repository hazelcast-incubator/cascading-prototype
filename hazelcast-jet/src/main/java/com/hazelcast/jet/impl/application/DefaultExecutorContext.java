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

package com.hazelcast.jet.impl.application;

import java.util.ArrayList;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.jet.api.executor.TaskExecutor;
import com.hazelcast.jet.api.application.ExecutorContext;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.executor.ApplicationTaskContext;
import com.hazelcast.jet.api.executor.SharedApplicationExecutor;
import com.hazelcast.jet.impl.executor.StateMachineTaskExecutorImpl;
import com.hazelcast.jet.impl.executor.DefaultApplicationTaskContext;


public class DefaultExecutorContext implements ExecutorContext {
    private final ApplicationTaskContext networkTaskContext;
    private final SharedApplicationExecutor networkExecutor;
    private final TaskExecutor containerStateMachineExecutor;
    private final SharedApplicationExecutor processingExecutor;
    private final TaskExecutor applicationStateMachineExecutor;
    private final ApplicationTaskContext applicationTaskContext;
    private final TaskExecutor applicationMasterStateMachineExecutor;

    public DefaultExecutorContext(
            String name,
            JetApplicationConfig jetApplicationConfig,
            NodeEngine nodeEngine,
            SharedApplicationExecutor networkExecutor,
            SharedApplicationExecutor processingExecutor) {
        this.networkExecutor = networkExecutor;
        this.processingExecutor = processingExecutor;
        int awaitingTimeOut = jetApplicationConfig.getJetSecondsToAwait();

        this.networkTaskContext = new DefaultApplicationTaskContext(new ArrayList<Task>());
        this.applicationTaskContext = new DefaultApplicationTaskContext(new ArrayList<Task>());

        this.containerStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-container-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.applicationStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-application-state_machine", 1, awaitingTimeOut, nodeEngine);

        this.applicationMasterStateMachineExecutor =
                new StateMachineTaskExecutorImpl(name + "-application-master-state_machine", 1, awaitingTimeOut, nodeEngine);
    }

    @Override
    public TaskExecutor getApplicationStateMachineExecutor() {
        return this.applicationStateMachineExecutor;
    }

    @Override
    public TaskExecutor getDataContainerStateMachineExecutor() {
        return this.containerStateMachineExecutor;
    }

    @Override
    public TaskExecutor getApplicationMasterStateMachineExecutor() {
        return this.applicationMasterStateMachineExecutor;
    }

    @Override
    public SharedApplicationExecutor getNetworkExecutor() {
        return this.networkExecutor;
    }

    @Override
    public SharedApplicationExecutor getProcessingExecutor() {
        return this.processingExecutor;
    }

    @Override
    public ApplicationTaskContext getNetworkTaskContext() {
        return this.networkTaskContext;
    }

    @Override
    public ApplicationTaskContext getApplicationTaskContext() {
        return this.applicationTaskContext;
    }
}
