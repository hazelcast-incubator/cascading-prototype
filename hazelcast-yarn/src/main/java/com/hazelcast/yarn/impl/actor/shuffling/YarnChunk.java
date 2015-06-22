package com.hazelcast.yarn.impl.actor.shuffling;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;


public class YarnChunk implements DataSerializable {
    private byte[] data;

    public YarnChunk(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.data = in.readByteArray();
    }
}
