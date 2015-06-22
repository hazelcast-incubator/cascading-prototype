package com.hazelcast.yarn.wordcount.offheap;

import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.HDTuple;
import com.hazelcast.yarn.impl.tuple.DefaultHDTuple2;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class WordGeneratorProcessor implements TupleContainerProcessor<Object, Object, Object, Object> {
    private static final int NR = "\r".getBytes()[0];
    private static final int NN = "\n".getBytes()[0];
    private static final int SPACE = " ".getBytes()[0];

    private static boolean isDelimiter(byte b) {
        return b == SPACE || b == NR || b == NN;
    }

    @Override
    public void beforeProcessing(ContainerContext containerContext) {

    }

    @Override
    public boolean process(TupleInputStream<Object, Object> inputStream,
                           TupleOutputStream<Object, Object> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        long wgcTime = System.currentTimeMillis();
        for (Tuple<Object, Object> t : inputStream) {
            HDTuple hdTuple = (HDTuple) t;

            long valueAddress = hdTuple.valueAddress();
            long valueSize = hdTuple.valueObjectSize();

            int blockStart = -1;
            int lastLetter = -1;

            for (int i = 0; i < valueSize; i++) {
                byte b = YarnUtil.getUnsafe().getByte(valueAddress + i);

                if (!isDelimiter(b)) {
                    lastLetter = i;
                    if (blockStart < 0) {
                        blockStart = i;
                    }
                } else {
                    if (blockStart >= 0) {
                        Tuple<Object, Object> tt = new DefaultHDTuple2(
                                valueAddress + blockStart,
                                i - blockStart + 1
                        );

                        outputStream.consume(tt);
                        blockStart = -1;
                    }
                }
            }

            if (blockStart >= 0) {
                Tuple<Object, Object> tt = new DefaultHDTuple2(
                        valueAddress + lastLetter,
                        lastLetter - blockStart + 1
                );

                outputStream.consume(tt);
            }
        }

        System.out.println("wgp=" + (System.currentTimeMillis() - wgcTime));
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<Object, Object> outputStream,
                                     ContainerContext containerContext) throws Exception {
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory<Object, Object, Object, Object> {
        @Override
        public TupleContainerProcessor<Object, Object, Object, Object> getProcessor(Vertex vertex) {
            return new WordGeneratorProcessor();
        }
    }
}
