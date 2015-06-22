package com.hazelcast.yarn.impl.tap.source;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class HDTupleFileReader extends AbstractHazelcastReader<Object, Object> {
    private RandomAccessFile fileReader;

    public HDTupleFileReader(ApplicationContext applicationContext,
                             Vertex vertex,
                             TupleFactory tupleFactory,
                             String name) {
        super(applicationContext, name, 0, tupleFactory, vertex);
    }

    @Override
    protected void onClose() {
        if (this.fileReader != null) {
            try {
                this.fileReader.close();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }

    @Override
    protected void onOpen() {
        try {
            this.fileReader = new RandomAccessFile(getName(), "rw");

            if (this.fileReader.getChannel().size() > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "File " + getName() + " is too big to be used in this tuple. " +
                                "It's size should be less than " + Integer.MAX_VALUE
                );
            }

            this.iterator = new HDFileIterator(this.fileReader, getName());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
