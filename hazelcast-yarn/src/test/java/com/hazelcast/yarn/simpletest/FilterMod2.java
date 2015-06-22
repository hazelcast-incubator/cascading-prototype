package com.hazelcast.yarn.simpletest;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class FilterMod2 implements TupleContainerProcessor<Integer, String, Integer, String> {
    public static final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void beforeProcessing(ContainerContext containerContext) {
        counter.set(0);
    }

    @Override
    public boolean process(TupleInputStream<Integer, String> inputStream,
                           TupleOutputStream<Integer, String> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        counter.addAndGet(inputStream.size());
        outputStream.consumeStream(inputStream);
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Integer, String> outputStream, ContainerContext containerContext) {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new FilterMod2();
        }
    }
}
