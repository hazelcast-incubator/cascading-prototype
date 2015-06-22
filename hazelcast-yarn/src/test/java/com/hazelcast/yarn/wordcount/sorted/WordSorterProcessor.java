package com.hazelcast.yarn.wordcount.sorted;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.impl.tuple.Tuple2;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;


public class WordSorterProcessor implements TupleContainerProcessor<String, Integer, String, Integer> {
    private Map<String, Integer> sortedMap;
    private Iterator<Map.Entry<String, Integer>> iterator;

    @Override
    public void beforeProcessing(ContainerContext containerContext) {
        if (containerContext.getVertex().getDescriptor().getTaskCount() > 1) {
            throw new IllegalStateException();
        }

        sortedMap = new TreeMap<String, Integer>();
    }

    @Override
    public boolean process(TupleInputStream<String, Integer> inputStream,
                           TupleOutputStream<String, Integer> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        for (Tuple<String, Integer> tuple : inputStream) {
            sortedMap.put(tuple.getKey(0), tuple.getValue(0));
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<String, Integer> outputStream,
                                     ContainerContext containerContext) throws Exception {
        if (iterator == null) {
            iterator = sortedMap.entrySet().iterator();
        }

        int idx = 0;
        int chunkSize = containerContext.getApplicationContext().getYarnApplicationConfig().getTupleChunkSize();

        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            outputStream.consume(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));

            if (idx == chunkSize) {
                return false;
            }

            idx++;
        }

        iterator = null;
        sortedMap = null;
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        @Override
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new WordSorterProcessor();
        }
    }
}
