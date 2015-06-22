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

package com.hazelcast.jet.impl.application;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.application.ExecutorContext;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.executor.ApplicationExecutor;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.executor.DefaultApplicationExecutor;

public class DefaultExecutorContext implements ExecutorContext {
    private final ApplicationExecutor networkExecutor;
    private final ApplicationExecutor processingExecutor;
    private final ApplicationExecutor applicationStateMachineExecutor;
    private final ApplicationExecutor containerStateMachineExecutor;
    private final ApplicationExecutor applicationMasterStateMachineExecutor;

    public DefaultExecutorContext(
            String name,
            JetApplicationConfig jetApplicationConfig,
            NodeEngine nodeEngine,
            ApplicationExecutor networkExecutor
    ) {
        int awaitingTimeOut = jetApplicationConfig.getJetSecondsToAwait();

        this.networkExecutor = networkExecutor;

        this.processingExecutor = new DefaultApplicationExecutor(
                name + "-application_executor",
                jetApplicationConfig.getMaxProcessingThreads(),
                jetApplicationConfig.getJetSecondsToAwait(),
                nodeEngine
        );

        this.applicationStateMachineExecutor =
                new StateMachineExecutor(name + "-application-state_machine", 1, awaitingTimeOut, nodeEngine);
        this.applicationMasterStateMachineExecutor =
                new StateMachineExecutor(name + "-application-master-state_machine", 1, awaitingTimeOut, nodeEngine);
        this.containerStateMachineExecutor =
                new StateMachineExecutor(name + "-container-state_machine", 1, awaitingTimeOut, nodeEngine);
    }

    @Override
    public ApplicationExecutor getProcessingExecutor() {
        return this.processingExecutor;
    }

    @Override
    public ApplicationExecutor getNetworkExecutor() {
        return this.networkExecutor;
    }

    @Override
    public ApplicationExecutor getApplicationStateMachineExecutor() {
        return this.applicationStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getDataContainerStateMachineExecutor() {
        return this.containerStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getApplicationMasterStateMachineExecutor() {
        return this.applicationMasterStateMachineExecutor;
    }
}
