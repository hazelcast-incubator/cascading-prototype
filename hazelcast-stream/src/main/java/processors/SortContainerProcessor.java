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

package processors;

import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Comparator;

import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;

//CHECKSTYLE:OFF
public class SortContainerProcessor implements ContainerProcessor<Object, Object> {
    private final Object[] arguments;
    private SortedMap<Object, Object> sortedMap;

    public SortContainerProcessor(Object[] arguments) {
        this.arguments = arguments;
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        if ((arguments == null) || (arguments.length == 0)) {
            this.sortedMap = new TreeMap<Object, Object>();
        } else {
            this.sortedMap = new TreeMap<Object, Object>((Comparator) arguments[arguments.length - 1]);
        }
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<Object> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Object obj : inputStream) {
            Tuple tuple = (Tuple) obj;
            this.sortedMap.put(tuple.getKey(0), obj);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Object> outputStream, ProcessorContext processorContext) throws Exception {
        for (Object key : this.sortedMap.keySet()) {
            outputStream.consume(this.sortedMap.get(key));
        }

        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class SortContainerProcessorFactory implements ContainerProcessorFactory<Object, Object> {
        private final Object[] arguments;

        public SortContainerProcessorFactory() {
            this.arguments = null;
        }

        public SortContainerProcessorFactory(Object... arguments) {
            this.arguments = arguments;
        }

        @Override
        public ContainerProcessor<Object, Object> getProcessor(Vertex vertex) {
            return new SortContainerProcessor(this.arguments);
        }
    }
}
//CHECKSTYLE:ON

