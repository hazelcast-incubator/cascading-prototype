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

package com.hazelcast.jet.api.application;

import java.util.Map;
import java.util.List;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.dag.DAG;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.api.container.CounterKey;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.container.ContainerListener;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.api.application.localization.LocalizationStorage;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

/***
 * ApplicationContext which represents context of the certain application.
 * Used during calculation process
 */
public interface ApplicationContext extends IOContext {
    /***
     * @return direct acyclic graph corresponding to application
     */
    DAG getDAG();

    /***
     * @return name of the application
     */
    String getName();

    /***
     * @return node's address which created application
     */
    Address getOwner();

    /***
     * @return node engine of corresponding to the current node
     */
    NodeEngine getNodeEngine();

    AtomicInteger getContainerIDGenerator();

    ApplicationMaster getApplicationMaster();

    boolean validateOwner(Address applicationOwner);

    LocalizationStorage getLocalizationStorage();

    JetApplicationConfig getJetApplicationConfig();

    ApplicationStateMachine getApplicationStateMachine();

    void registerApplicationListener(ApplicationListener applicationListener);

    ConcurrentMap<String, List<ContainerListener>> getContainerListeners();

    List<ApplicationListener> getApplicationListeners();

    Map<Address, Address> getHzToJetAddressMapping();

    Map<Address, SocketWriter> getSocketWriters();

    Map<Address, SocketReader> getSocketReaders();

    Address getLocalJetAddress();

    void registerContainerListener(String vertexName,
                                   ContainerListener containerListener);

    <T> void putApplicationVariable(String variableName, T variable);

    <T> T getApplicationVariable(String variableName);

    void cleanApplicationVariable(String variableName);

    ExecutorContext getExecutorContext();

    Map<CounterKey, Accumulator> getAccumulators();

    void registerAccumulators(ConcurrentMap<CounterKey, Accumulator> accumulatorMap);
}
