package com.hazelcast.yarn.impl.operation.application;

import java.io.IOException;

import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class LocalizationChunkOperation extends AbstractYarnApplicationRequestOperation {
    private Chunk chunk;

    public LocalizationChunkOperation() {

    }

    public LocalizationChunkOperation(String name, Chunk chunk) {
        super(name);
        this.chunk = chunk;
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        applicationContext.getLocalizationStorage().receiveFileChunk(this.chunk);
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(chunk);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        chunk = in.readObject();
    }
}
