package com.hazelcast.yarn.wordcount;


import java.util.Iterator;
import java.util.StringTokenizer;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessor;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class WordGeneratorProcessor implements TupleContainerProcessor<String, String, String, Integer> {
    private int idx;
    private StringTokenizer stringTokenizer;
    private Iterator<Tuple<String, String>> iterator;

    private boolean processStringTokenizer(TupleOutputStream<String, Integer> outputStream,
                                           ContainerContext containerContext) throws Exception {
        while (this.stringTokenizer.hasMoreElements()) {
            String word = this.stringTokenizer.nextToken();

            outputStream.consume(
                    containerContext.getTupleFactory().tuple(word, 1)
            );

            this.idx++;

            if (this.idx == containerContext.getApplicationContext().getYarnApplicationConfig().getTupleChunkSize()) {
                this.idx = 0;
                return false;
            }
        }

        this.idx = 0;
        this.stringTokenizer = null;
        return true;
    }

    @Override
    public void beforeProcessing(ContainerContext containerContext) {
        this.idx = 0;
        this.iterator = null;
        this.stringTokenizer = null;
    }

    @Override
    public boolean process(TupleInputStream<String, String> inputStream,
                           TupleOutputStream<String, Integer> outputStream,
                           String sourceName,
                           ContainerContext containerContext) throws Exception {
        if (this.stringTokenizer != null) {
            this.processStringTokenizer(outputStream, containerContext);
            return false;
        }

        if (this.iterator == null) {
            this.iterator = inputStream.iterator();
        }

        while (iterator.hasNext()) {
            Tuple<String, String> t = iterator.next();

            String text = t.getValue(0);
            this.stringTokenizer = new StringTokenizer(text);

            if (!this.processStringTokenizer(outputStream, containerContext)) {
                return false;
            }
        }

        this.iterator = null;
        return true;
    }

    @Override
    public boolean finalizeProcessor(TupleOutputStream<String, Integer> outputStream,
                                     ContainerContext containerContext) throws Exception {
        if (this.stringTokenizer != null) {
            this.processStringTokenizer(outputStream, containerContext);
            return false;
        }

        this.stringTokenizer = null;
        return true;
    }

    public static class Factory implements TupleContainerProcessorFactory<String, String, String, Integer> {
        @Override
        public TupleContainerProcessor<String, String, String, Integer> getProcessor(Vertex vertex) {
            return new WordGeneratorProcessor();
        }
    }
}
