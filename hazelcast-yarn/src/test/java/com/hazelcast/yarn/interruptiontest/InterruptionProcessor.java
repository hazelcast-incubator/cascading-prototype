package com.hazelcast.yarn.interruptiontest;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;

import java.util.concurrent.locks.LockSupport;

import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;


public class InterruptionProcessor implements TupleContainerProcessor<Object, Object, Object, Object> {
    @Override
    public void beforeProcessing(ContainerContext containerContext) {

    }

    @Override
    public boolean process(TupleInputStream<Object, Object> inputStream,
                           TupleOutputStream<Object, Object> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        for (Tuple<Object, Object> t : inputStream) {
            LockSupport.parkNanos(10);
            outputStream.consume(t);
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Object, Object> outputStream,
                                     ContainerContext containerContext) throws Exception {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new InterruptionProcessor();
        }
    }
}
