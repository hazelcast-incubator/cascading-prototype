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


import java.util.function.Function;
import java.util.function.Predicate;

import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.processor.ContainerProcessor;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;

//CHECKSTYLE:OFF
public class MapContainerProcessor implements ContainerProcessor<Object, Object> {
    private final Object argument;

    private boolean isArray;

    public MapContainerProcessor(Object argument) {
        this.argument = argument;
        this.isArray = argument.getClass().isArray();
    }

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {

    }

    private Object processArgument(Object argument, Object object) {
        if (argument instanceof Predicate) {
            if (!((Predicate) argument).test(object)) {
                return null;
            } else {
                return object;
            }
        } else if (argument instanceof Function) {
            return ((Function) argument).apply(object);
        }

        throw new IllegalStateException("Unsupported argument: " + argument);
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<Object> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Object object : inputStream) {
            Object result = null;
            Tuple<Object, Object> tuple = (Tuple<Object, Object>) object;
            Object payLoad = tuple.getValue(0);

            if (this.isArray) {
                for (Object argument : (Object[]) this.argument) {
                    result = processArgument(argument, payLoad);
                }
            } else {
                result = this.processArgument(this.argument, payLoad);
            }

            if (result != null) {
                tuple.setValue(0, result);
                outputStream.consume(tuple);
            }
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Object> outputStream, ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class MapContainerProcessorFactory implements ContainerProcessorFactory<Object, Object> {
        private final Object arguments;

        public MapContainerProcessorFactory(Object arguments) {
            this.arguments = arguments;
        }

        @Override
        public ContainerProcessor<Object, Object> getProcessor(Vertex vertex) {
            return new MapContainerProcessor(this.arguments);
        }
    }
}
//CHECKSTYLE:ON
