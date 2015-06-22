/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package org;

import java.util.Map;
import java.util.Iterator;

import com.hazelcast.jet.api.container.ProcessorContext;
import gnu.trove.map.TObjectIntMap;
import com.hazelcast.jet.api.dag.Vertex;
import gnu.trove.map.hash.TObjectIntHashMap;
import com.hazelcast.jet.api.data.tuple.Tuple;
import com.hazelcast.jet.impl.data.tuple.Tuple2;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.data.io.ConsumerOutputStream;
import com.hazelcast.jet.api.processor.ContainerProcessor;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;

public class WordCounterProcessor implements ContainerProcessor<String, Tuple<String, Integer>> {
    private static final AtomicInteger processCounter = new AtomicInteger(0);

    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private final static Map<Integer, TObjectIntMap<Object>> caches = new Int2ObjectHashMap<TObjectIntMap<Object>>();

    private final int index;

    private volatile int taskCount;

    private Iterator<Object> finalizationIterator;

    private TObjectIntMap<Object> cache = new TObjectIntHashMap<Object>();

    public WordCounterProcessor() {
        this.index = processCounter.getAndIncrement();
    }


    @Override
    public void beforeProcessing(ProcessorContext processorContext) {
        taskCounter.set(processorContext.getVertex().getDescriptor().getTaskCount());
        this.taskCount = processorContext.getVertex().getDescriptor().getTaskCount();
        caches.put(index, cache);
    }

    @Override
    public boolean process(ProducerInputStream<String> inputStream,
                           ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (String word : inputStream) {
            this.cache.adjustOrPutValue(word, 1, 1);
        }

        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Tuple<String, Integer>> outputStream,
                                     ProcessorContext processorContext) throws Exception {
        boolean finalized = false;

        try {
            if (finalizationIterator == null) {
                this.finalizationIterator = this.cache.keySet().iterator();
            }

            int idx = 0;
            int chunkSize = processorContext.getConfig().getChunkSize();

            while (this.finalizationIterator.hasNext()) {
                String word = (String) this.finalizationIterator.next();

                if (checkLeft(word)) {
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

    @Override
    public void afterProcessing(ProcessorContext processorContext) {

    }

    private void clearCaches() {
        if (taskCounter.decrementAndGet() <= 0) {
            for (int i = 0; i < taskCount; i++) {
                System.out.println("Cache " + i + " cleared");
                caches.get(i).clear();
            }

            System.out.println("Caches cleared");
            processCounter.set(0);
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

    public static class Factory implements ContainerProcessorFactory {
        @Override
        public ContainerProcessor getProcessor(Vertex vertex) {
            return new WordCounterProcessor();
        }
    }
}
