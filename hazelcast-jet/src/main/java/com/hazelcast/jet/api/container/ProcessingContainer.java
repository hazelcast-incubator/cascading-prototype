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

import java.util.Map;
import java.util.List;

import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.container.task.TaskEvent;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerState;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerResponse;

public interface ProcessingContainer
        extends Container<ProcessingContainerEvent, ProcessingContainerState, ProcessingContainerResponse> {
    ContainerProcessorFactory getContainerProcessorFactory();

    List<DataChannel> getInputChannels();

    List<DataChannel> getOutputChannels();

    void addInputChannel(DataChannel channel);

    void addOutputChannel(DataChannel channel);

    void handleTaskEvent(ContainerTask containerTask, TaskEvent event);

    void handleTaskEvent(ContainerTask containerTask, TaskEvent event, Throwable error);

    Map<Integer, ContainerTask> getTasksCache();

    ContainerTask[] getContainerTasks();

    Vertex getVertex();

    void start() throws Exception;

    void destroy() throws Exception;

    void interrupt(Throwable error);
}
