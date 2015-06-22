package com.hazelcast.yarn.wordcount;

import java.util.Map;
import java.util.Iterator;

import gnu.trove.map.TObjectIntMap;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.impl.tuple.Tuple2;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;


public class WordCounterProcessor implements TupleContainerProcessor<String, Integer, String, Integer> {
    private static final AtomicInteger processCounter = new AtomicInteger(0);

    private final int index;

    private volatile int taskCount;

    public static volatile long time;

    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private TObjectIntMap<Object> cache = new TObjectIntHashMap<Object>();

    private final static Map<Integer, TObjectIntMap<Object>> caches = new Int2ObjectHashMap<TObjectIntMap<Object>>();

    private Iterator<Object> finalizationIterator;

    public WordCounterProcessor() {
        this.index = processCounter.getAndIncrement();
    }

    @Override
    public void beforeProcessing(ContainerContext containerContext) {
        taskCounter.set(containerContext.getVertex().getDescriptor().getTaskCount());
        taskCount = containerContext.getVertex().getDescriptor().getTaskCount();
        caches.put(index, cache);
    }

    @Override
    public boolean process(TupleInputStream<String, Integer> inputStream,
                           TupleOutputStream<String, Integer> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        for (Tuple<String, Integer> tuple : inputStream) {
            this.cache.adjustOrPutValue(tuple.getKey(0), tuple.getValue(0), tuple.getValue(0));
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<String, Integer> outputStream,
                                     ContainerContext containerContext) throws Exception {
        boolean finalized = false;

        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.keySet().iterator();
            }

            int idx = 0;
            int chunkSize = containerContext.getApplicationContext().getYarnApplicationConfig().getTupleChunkSize();

            while (this.finalizationIterator.hasNext()) {
                String word = (String) this.finalizationIterator.next();

                if (this.checkLeft(word)) {
                    continue;
                }

                int count = (int) merge(word);

                outputStream.consume(new Tuple2<String, Integer>(word, count));

                if (idx == chunkSize - 1) {
                    break;
                }

                idx++;
            }

            finalized = !this.finalizationIterator.hasNext();
        } finally {
            if (finalized) {
                this.finalizationIterator = null;
                clearCaches();
            }
        }

        return finalized;
    }

    private void clearCaches() {
        if (taskCounter.decrementAndGet() <= 0) {
            for (int i = 0; i < taskCount; i++) {
                System.out.println("Cache " + i + " cleared");
                caches.get(i).clear();
            }

            System.out.println("Caches cleared");
            caches.clear();
        }
    }

    private long merge(Object word) {
        long result = 0;

        for (int i = index; i < taskCount; i++) {
            if (caches.get(i).containsKey(word)) {
                result += caches.get(i).get(word);
            }
        }

        return result;
    }

    private boolean checkLeft(Object tuple) {
        for (int i = 0; i < index; i++) {
            if (caches.get(i).containsKey(tuple)) {
                return true;
            }
        }

        return false;
    }

    public static class Factory implements TupleContainerProcessorFactory {
        @Override
        public TupleContainerProcessor getProcessor(Vertex vertex) {
            return new WordCounterProcessor();
        }
    }
}
