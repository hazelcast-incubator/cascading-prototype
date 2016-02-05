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

package com.hazelcast.jet.impl.container.task;

import java.io.Serializable;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.jet.api.application.IOContext;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.data.io.DataType;
import com.hazelcast.jet.spi.data.io.ObjectReaderFactory;
import com.hazelcast.jet.spi.data.io.ObjectWriterFactory;
import com.hazelcast.jet.spi.executor.TaskContext;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.application.IOContextImpl;

public class DefaultTaskContext implements TaskContext {
    private final int taskCount;
    private final int taskNumber;
    private final IOContext ioContext;
    private final ConcurrentMap<CounterKey, Accumulator> accumulatorMap;

    public DefaultTaskContext(int taskCount,
                              int taskNumber,
                              ApplicationContext applicationContext) {
        this.taskCount = taskCount;
        this.taskNumber = taskNumber;
        this.ioContext = new IOContextImpl();
        this.accumulatorMap = new ConcurrentHashMap<CounterKey, Accumulator>();
        applicationContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public int getTaskCount() {
        return this.taskCount;
    }

    @Override
    public int getTaskNumber() {
        return this.taskNumber;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey) {
        return this.accumulatorMap.get(counterKey);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(CounterKey counterKey, Accumulator<V, R> accumulator) {
        this.accumulatorMap.put(counterKey, accumulator);
    }

    @Override
    public void registerDataType(DataType dataType) {
        this.ioContext.registerDataType(dataType);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.ioContext.getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.ioContext.getObjectWriterFactory();
    }
}
