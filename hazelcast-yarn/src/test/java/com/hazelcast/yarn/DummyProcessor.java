package com.hazelcast.yarn;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class DummyProcessor implements TupleContainerProcessor<Integer, String, Integer, String> {
    @Override
    public void beforeProcessing(ContainerContext containerContext) {

    }

    @Override
    public boolean process(TupleInputStream<Integer, String> inputStream,
                           TupleOutputStream<Integer, String> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        outputStream.consumeStream(inputStream);
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Integer, String> outputStream, ContainerContext containerContext) throws Exception {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new DummyProcessor();
        }
    }
}
