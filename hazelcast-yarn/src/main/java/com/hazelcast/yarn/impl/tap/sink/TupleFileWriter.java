package com.hazelcast.yarn.impl.tap.sink;

import java.io.IOException;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class TupleFileWriter extends AbstractHazelcastWriter {
    private final FileWrapper fileWrapper;


    public TupleFileWriter(ContainerContext containerContext, FileWrapper fileWrapper, int partitionID) {
        super(containerContext, partitionID, SinkTapWriteStrategy.CLEAR_AND_REPLACE);
        this.fileWrapper = fileWrapper;
        this.fileWrapper.registerWriter();
    }

    @Override
    protected void processChunk(TupleInputStream stream) {
        try {
            StringBuilder sb = new StringBuilder();

            for (Object o : stream) {
                Tuple t = (Tuple) o;

                for (int i = 0; i < t.keySize(); i++) {
                    sb.append(t.getKey(i).toString()).append(" ");
                }

                for (int i = 0; i < t.valueSize(); i++) {
                    sb.append(t.getValue(i).toString()).append(" ");
                }

                sb.append("\r\n");
            }

            if (sb.length() > 0) {
                this.fileWrapper.getFileWriter().write(sb.toString());
                this.fileWrapper.getFileWriter().flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onOpen() {
        this.fileWrapper.open(getSinkTapWriteStrategy());
    }

    @Override
    protected void onClose() {
        this.fileWrapper.close();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}

