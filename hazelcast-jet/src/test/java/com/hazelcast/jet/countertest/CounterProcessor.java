package com.hazelcast.jet.countertest;

import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.api.container.ProcessorContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.processor.ContainerProcessor;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;

public class CounterProcessor implements ContainerProcessor<Object, Object> {
    private static final String OBJECTS_COUNTER = "objectsCounter";

    private LongCounter longCounter;

    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        this.longCounter = new LongCounter();
        processorContext.setAccumulator(OBJECTS_COUNTER, this.longCounter);
    }

    @Override
    public boolean process(ProducerInputStream<Object> inputStream,
                           ConsumerOutputStream<Object> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        this.longCounter.add(inputStream.size());
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Object> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        return true;
    }

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    public static class CounterProcessorFactory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new CounterProcessor();
        }
    }
}
