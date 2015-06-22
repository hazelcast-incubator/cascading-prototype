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

package com.hazelcast.jet.impl.container.events;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.task.TaskEvent;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerListener;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.NotifyContainerExecutionError;

public class TaskEventExecutionError extends AbstractEventProcessor {
    protected TaskEventExecutionError(AtomicInteger completedTasks,
                                      AtomicInteger interruptedTasks,
                                      AtomicInteger readyForFinalizationTasksCounter,
                                      ContainerTask[] containerTasks,
                                      ContainerContext containerContext,
                                      DataContainer dataContainer) {
        super(
                completedTasks,
                interruptedTasks,
                readyForFinalizationTasksCounter,
                containerTasks,
                containerContext,
                dataContainer
        );
    }

    public void process(ContainerTask containerTask,
                        TaskEvent event,
                        Throwable error) {
        try {
            try {
                this.applicationMaster.handleContainerRequest(
                        new NotifyContainerExecutionError(this.container, error)
                );
            } finally {
                this.applicationMaster.invalidateApplicationInCluster(error);
            }
        } finally {
            invokeContainerListeners(ContainerListener.FAILURE_LISTENER_CALLER, error);
        }
    }
}
