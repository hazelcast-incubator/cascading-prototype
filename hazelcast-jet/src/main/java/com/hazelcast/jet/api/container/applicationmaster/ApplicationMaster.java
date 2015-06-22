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

package com.hazelcast.jet.api.container.applicationmaster;


import java.util.Map;
import java.util.List;

import com.hazelcast.config.Config;
import com.hazelcast.nio.Address;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.api.container.Container;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ExecutionErrorHolder;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;


public interface ApplicationMaster extends Container<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {
    void handleContainerInterrupted(DataContainer tapContainer);

    void handleContainerCompleted(DataContainer tapContainer);

    void registerExecution();

    void registerInterruption();

    void invalidateApplicationLocal(Object reason);

    BlockingQueue<Object> getExecutionMailBox();

    BlockingQueue<Object> getInterruptionMailBox();

    void setExecutionError(ExecutionErrorHolder executionError);

    DataContainer getContainerByVertex(Vertex vertex);

    void registerContainer(Vertex vertex, DataContainer container);

    Map<Integer, DataContainer> getContainersCache();

    List<DataContainer> containers();

    void registerShufflingReceiver(int taskID, ContainerContext containerContext, Address address, ShufflingReceiver receiver);

    void registerShufflingSender(int taskID, ContainerContext containerContext, Address address, ShufflingSender sender);

    BlockingQueue<Object> synchronizeWithOtherNodes(DataContainer container);

    void setDag(DAG dag);

    Config getConfig();

    DAG getDag();

    String getApplicationName();

    void addToExecutionMailBox(Object object);

    int notifyContainersExecution(JetPacket packet);

    void invalidateApplicationInCluster(Object reason);

    void startNetWorkEngine();

    void deployNetworkEngine();
}
