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

import java.io.Serializable;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.io.DataType;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.data.tuple.TupleFactory;
import com.hazelcast.jet.api.data.io.ObjectReaderFactory;
import com.hazelcast.jet.api.data.io.ObjectWriterFactory;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.application.ApplicationListener;


public interface ContainerDescriptor {
    NodeEngine getNodeEngine();

    String getApplicationName();

    int getID();

    Vertex getVertex();

    DAG getDAG();

    TupleFactory getTupleFactory();

    JetApplicationConfig getConfig();

    void registerContainerListener(String vertexName,
                                   ContainerListener containerListener);

    void registerApplicationListener(ApplicationListener applicationListener);

    <T> void putApplicationVariable(String variableName, T variable);

    <T> T getApplicationVariable(String variableName);

    void cleanApplicationVariable(String variableName);

    void registerDataType(DataType dataType);

    ObjectReaderFactory getObjectReaderFactory();

    ObjectWriterFactory getObjectWriterFactory();

    <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey);

    <V, R extends Serializable> void setAccumulator(CounterKey counterKey, Accumulator<V, R> accumulator);
}
