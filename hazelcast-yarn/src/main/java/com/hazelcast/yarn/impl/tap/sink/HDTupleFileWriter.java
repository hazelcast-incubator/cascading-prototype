package com.hazelcast.yarn.impl.tap.sink;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;


public class HDTupleFileWriter extends AbstractHazelcastWriter {
    private final HDFileManager fileManager;

    public HDTupleFileWriter(ContainerContext containerContext, HDFileManager fileManager) {
        super(containerContext, 0, SinkTapWriteStrategy.CLEAR_AND_REPLACE);
        this.fileManager = fileManager;
        this.fileManager.registerWriter();
    }

    @Override
    protected void processChunk(TupleInputStream stream) {
//        try {
//            long time = System.currentTimeMillis();
//            for (Object o : stream) {
//                Tuple t = (Tuple) o;
//                ByteBuffer bb = (ByteBuffer) t.getValue(0);
//
//                try {
//                    this.fileManager.getFileWriter().getChannel().write(bb);
//                } finally {
//                    ((DirectBuffer) bb).cleaner().clean();
//                }
//            }
//            System.out.println("File=" + (System.currentTimeMillis() - time));
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    protected void onOpen() {
        this.fileManager.open(this.getSinkTapWriteStrategy());
    }

    @Override
    protected void onClose() {
        this.fileManager.close();
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
