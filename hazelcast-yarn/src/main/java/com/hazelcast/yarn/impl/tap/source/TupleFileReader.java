package com.hazelcast.yarn.impl.tap.source;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class TupleFileReader extends AbstractHazelcastReader<String, String> {
    private FileReader fileReader;

    public TupleFileReader(ApplicationContext applicationContext,
                           Vertex vertex,
                           int partitionId,
                           TupleFactory tupleFactory,
                           String name) {
        super(applicationContext, name, partitionId, tupleFactory, vertex);
    }

    @Override
    protected void onOpen() {
        try {
            File f = new File(this.getName());

            if (f.length() > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "File " + getName() + " is too big to be used in this tuple. " +
                                "It's size should be less than " + Integer.MAX_VALUE
                );
            }

            this.fileReader = new FileReader(f);
            this.iterator = new FileIterator(this.fileReader, getTupleFactory(), getName());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onClose() {
        if (this.fileReader != null) {
            try {
                this.iterator = null;
                this.fileReader.close();
                this.fileReader = null;
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }
}