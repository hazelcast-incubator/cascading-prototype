package com.hazelcast.yarn.impl.tap.source;

import java.util.Iterator;
import java.lang.reflect.Method;
import java.io.RandomAccessFile;

import sun.nio.ch.FileChannelImpl;
import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.impl.tuple.DefaultHDTuple2;

public class HDFileIterator implements Iterator<Tuple<Object, Object>> {
    private boolean hasNext = true;
    private final String name;
    private final RandomAccessFile fileReader;

    public HDFileIterator(RandomAccessFile fileReader, String name) {
        this.name = name;
        this.fileReader = fileReader;
    }

    @Override
    public boolean hasNext() {
        return this.hasNext;
    }

    @Override
    public Tuple<Object, Object> next() {
        try {
            this.hasNext = false;

            Method map0 = FileChannelImpl.class.getDeclaredMethod(
                    "map0", int.class, long.class, long.class);

            map0.setAccessible(true);

            long size = this.fileReader.getChannel().size();
            long address = (Long) map0.invoke(this.fileReader.getChannel(), 1, 0L, size);

            byte[] nameBytes = this.name.getBytes();
            long keyAddress = YarnUtil.getUnsafe().allocateMemory(nameBytes.length);

            for (int i = 0; i < nameBytes.length; i++) {
                YarnUtil.getUnsafe().putByte(keyAddress + i, nameBytes[i]);
            }

            return new DefaultHDTuple2(keyAddress, nameBytes.length, address, size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
