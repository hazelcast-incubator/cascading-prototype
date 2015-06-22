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

package com.hazelcast.jet.impl.processor.context;

import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.io.DataType;
import com.hazelcast.jet.api.data.tuple.TupleFactory;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.data.io.ObjectReaderFactory;
import com.hazelcast.jet.api.data.io.ObjectWriterFactory;
import com.hazelcast.jet.api.container.ContainerListener;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.application.ApplicationListener;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultContainerContext implements ContainerContext {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final TupleFactory tupleFactory;
    private final ApplicationContext applicationContext;
    private final ConcurrentMap<String, Accumulator> accumulatorMap;

    public DefaultContainerContext(NodeEngine nodeEngine,
                                   ApplicationContext applicationContext,
                                   int id,
                                   Vertex vertex,
                                   TupleFactory tupleFactory) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.tupleFactory = tupleFactory;
        this.applicationContext = applicationContext;
        this.accumulatorMap = new ConcurrentHashMap<String, Accumulator>();
        applicationContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public String getApplicationName() {
        return this.applicationContext.getName();
    }

    @Override
    public int getID() {
        return this.id;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public DAG getDAG() {
        return this.applicationContext.getDAG();
    }

    @Override
    public TupleFactory getTupleFactory() {
        return this.tupleFactory;
    }

    @Override
    public JetApplicationConfig getConfig() {
        return this.applicationContext.getJetApplicationConfig();
    }

    @Override
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        this.applicationContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerApplicationListener(ApplicationListener applicationListener) {
        this.applicationContext.registerApplicationListener(applicationListener);
    }

    @Override
    public <T> void putApplicationVariable(String variableName, T variable) {
        this.applicationContext.putApplicationVariable(variableName, variable);
    }

    @Override
    public <T> T getApplicationVariable(String variableName) {
        return this.applicationContext.getApplicationVariable(variableName);
    }

    public void cleanApplicationVariable(String variableName) {
        this.applicationContext.cleanApplicationVariable(variableName);
    }

    @Override
    public void registerDataType(DataType dataType) {
        applicationContext.registerDataType(dataType);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.applicationContext.getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.applicationContext.getObjectWriterFactory();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String counterName) {
        return this.accumulatorMap.get(counterName);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String counterName,
                                                           Accumulator<V, R> accumulator) {
        this.accumulatorMap.put(counterName, accumulator);
    }
}
