package com.hazelcast.yarn.mapreverser;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;

import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class Reverser implements TupleContainerProcessor<Integer, String, String, Integer> {
    public static final AtomicInteger ii = new AtomicInteger(0);

    @Override
    public void beforeProcessing(ContainerContext containerContext) {

    }

    @Override
    public boolean process(TupleInputStream<Integer, String> inputStream,
                           TupleOutputStream<String, Integer> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        for (Tuple tuple : inputStream) {
            ii.incrementAndGet();
            Object key = tuple.getKey(0);
            Object value = tuple.getValue(0);
            tuple.setKey(0, value);
            tuple.setValue(0, key);
            outputStream.consume(tuple);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<String, Integer> outputStream, ContainerContext containerContext) {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new Reverser();
        }
    }
}
