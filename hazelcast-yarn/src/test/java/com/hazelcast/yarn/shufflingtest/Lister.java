package com.hazelcast.yarn.shufflingtest;

import java.util.Map;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class Lister implements TupleContainerProcessor<Integer, String, Integer, String> {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Map<Integer, Tuple<Integer, String>> list = new ConcurrentSkipListMap<Integer, Tuple<Integer, String>>();

    private static volatile String activeNode;

    @Override
    public void beforeProcessing(ContainerContext containerContext) {
        activeNode = null;
        counter.set(0);
    }

    @Override
    public boolean process(TupleInputStream<Integer, String> inputStream,
                           TupleOutputStream<Integer, String> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        activeNode = containerContext.getNodeEngine().getHazelcastInstance().getName();

        for (Tuple<Integer, String> tuple : inputStream) {
            list.put(tuple.getKey(0), tuple);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Integer, String> outputStream, ContainerContext containerContext) throws Exception {
        if ((activeNode != null) && (activeNode.equals(containerContext.getNodeEngine().getHazelcastInstance().getName()))) {
            if (counter.incrementAndGet() >= containerContext.getVertex().getDescriptor().getTaskCount()) {
                System.out.println("Eventually I am writing");

                for (Map.Entry<Integer, Tuple<Integer, String>> integerTupleEntry : list.entrySet()) {
                    outputStream.consume(integerTupleEntry.getValue());
                }

                System.out.println("Eventually I've written");
            }
        }

        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new Lister();
        }
    }
}
