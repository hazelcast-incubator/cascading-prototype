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

package com.hazelcast.jet.simpletest;

import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.tuple.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessor;
import com.hazelcast.jet.api.processor.tuple.TupleContainerProcessorFactory;


public class FilterMod2 implements TupleContainerProcessor<Integer, String, Integer, String> {
    public static final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        counter.set(0);
    }

    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        counter.addAndGet(inputStream.size());
        outputStream.consumeStream(inputStream);
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<Integer, String>> outputStream, ProcessorContext processorContext) {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new FilterMod2();
        }
    }
}
