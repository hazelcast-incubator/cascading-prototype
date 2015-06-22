package com.hazelcast.yarn.impl;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.yarn.impl.application.LocalizationResourceDescriptor;

public class Chunk implements DataSerializable {
    private byte[] bytes;
    private long length;
    private LocalizationResourceDescriptor descriptor;

    public Chunk() {

    }

    public Chunk(byte[] bytes, LocalizationResourceDescriptor descriptor, long length) {
        this.bytes = bytes;
        this.length = length;
        this.descriptor = descriptor;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public LocalizationResourceDescriptor getDescriptor() {
        return descriptor;
    }

    public long getLength() {
        return length;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(bytes);
        out.writeObject(this.descriptor);
        out.writeLong(length);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        bytes = in.readByteArray();
        descriptor = in.readObject();
        length = in.readLong();
    }
}
