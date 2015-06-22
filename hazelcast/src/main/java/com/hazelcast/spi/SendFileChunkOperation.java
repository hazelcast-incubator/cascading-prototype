package com.hazelcast.spi;

public class SendFileChunkOperation extends AbstractOperation {
    private byte[] chunk;
    private int chunkSize;
    private String fileName;
    private long chunkAmount;

    @Override
    public void run() throws Exception {

    }
}
