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

package com.hazelcast.jet.interruptiontest;


import java.util.List;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.tuple.Tuple;

import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessorFactory;
import com.hazelcast.jet.api.data.io.ProducerInputStream;

public class CachingProcessor implements TupleContainerProcessor<Object, Object, Object, Object> {
    private static final List<Tuple> cache = new CopyOnWriteArrayList<Tuple>();

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean process(ProducerInputStream<Tuple<Object, Object>> inputStream,
                           ConsumerOutputStream<Tuple<Object, Object>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Tuple t : inputStream) {
            cache.add(t);
        }

        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream outputStream, ProcessorContext processorContext) throws Exception {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new InterruptionProcessor();
        }
    }
}
