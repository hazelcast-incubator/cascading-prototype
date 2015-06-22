package com.hazelcast.yarn.wordcount.offheap;

import java.nio.ByteBuffer;
import java.util.Map;

import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.HDTuple;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.collection.Int2ObjectHashMap;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;
import com.hazelcast.yarn.impl.tuple.Tuple2;
import sun.nio.ch.DirectBuffer;

public class WordCounterProcessor implements TupleContainerProcessor<Object, Object, Object, Object> {
    private static final AtomicInteger processCounter = new AtomicInteger(0);

    private final int index;

    public static volatile long time;

    private static final AtomicInteger taskCounter = new AtomicInteger(0);

    private final Object2LongOffHeapMap cache = new Object2LongOffHeapMap(65536 * 256);

    private final static Map<Integer, Object2LongOffHeapMap> caches = new Int2ObjectHashMap<Object2LongOffHeapMap>();

    private volatile int taskCount;

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
    public boolean process(TupleInputStream<Object, Object> inputStream,
                           TupleOutputStream<Object, Object> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        long cTime = System.currentTimeMillis();

        for (Tuple<Object, Object> tuple : inputStream) {
            HDTuple hdTuple = (HDTuple) tuple;
            long count = this.cache.get(hdTuple.keyAddress(), hdTuple.keyObjectSize());

            if (count < 0) {
                this.cache.put(hdTuple.keyAddress(), hdTuple.keyObjectSize(), 1L);
            } else {
                this.cache.put(hdTuple.keyAddress(), hdTuple.keyObjectSize(), count + 1);
            }
        }

        System.out.println("cTime=" + (System.currentTimeMillis() - cTime) + " chunk=" + inputStream.size());
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Object, Object> outputStream,
                                     ContainerContext containerContext) throws Exception {
        try {
            System.out.println("Finalization=" + (System.currentTimeMillis() - time) + " size=" + this.cache.size());

            long tt = System.currentTimeMillis();

            Object2LongOffHeapMap.KeySetIterator it = new Object2LongOffHeapMap.KeySetIterator(this.cache);

            int totalSize = 0;

            while (it.hasNext()) {
                long keyAddress = it.nextKeyAddress();
                long keySize = it.getKeyDataSize();

                totalSize += keySize;
                totalSize += getCharSize(cache.get(keyAddress, keySize));
                totalSize += 3;
            }

            int position = 0;

            byte space = " ".getBytes()[0];
            byte nr = "\r".getBytes()[0];
            byte nn = "\n".getBytes()[0];

            ByteBuffer bb = ByteBuffer.allocateDirect(totalSize);

            it = new Object2LongOffHeapMap.KeySetIterator(this.cache);

            while (it.hasNext()) {
                long keyAddress = it.nextKeyAddress();
                long keySize = it.getKeyDataSize();

                if (this.checkLeft(keyAddress, keySize)) {
                    continue;
                }

                long count = merge(keyAddress, keySize);
                int size = getCharSize(count);
                long address = getAddress(count, size);

                if (keySize > 0) {
                    YarnUtil.getUnsafe().copyMemory(keyAddress, ((DirectBuffer) bb).address() + position, keySize);
                    position += keySize;
                    bb.position(position);
                }

                bb.put(space);

                YarnUtil.getUnsafe().copyMemory(address, ((DirectBuffer) bb).address() + position, size);
                position += size;
                bb.position(position);

                bb.put(nr);
                bb.put(nn);

                position = bb.position();
            }

            bb.position(0);
            outputStream.consume(new Tuple2<Object, Object>("", bb));
            System.out.println("FinalizationDone=" + (System.currentTimeMillis() - tt));
        } finally {
            try {
                //this.cache.clear();
            } finally {
                if (taskCounter.decrementAndGet() == 0) {
                    caches.clear();
                }
            }
        }

        return true;
    }

    private long getAddress(long count, int size) {
        long address = YarnUtil.getUnsafe().allocateMemory(size);
        int pointer = 1;

        do {
            YarnUtil.getUnsafe().putByte(size + address - pointer, (byte) (count % 10 + 48));
            count = count / 10;
            pointer++;
        } while (count > 0);

        return address;
    }

    private int getCharSize(long count) {
        int size = 0;

        do {
            size++;
            count = count / 10;
        } while (count > 0);

        return size;
    }

    private long merge(long keyAddress, long keySize) {
        long result = 0;

        for (int i = index; i < taskCount; i++) {
            long count = caches.get(i).get(keyAddress, keySize);

            if (count > 0) {
                result += count;
            }
        }

        return result;
    }

    private boolean checkLeft(long keyAddress, long keySize) {
        for (int i = 0; i < index; i++) {
            if (caches.get(i).get(keyAddress, keySize) > 0) {
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

