package com.hazelcast.yarn.impl.tap.source;


import java.io.FileReader;
import java.util.Iterator;
import java.io.IOException;
import java.io.LineNumberReader;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.TupleFactory;


public class FileIterator implements Iterator<Tuple<String, String>> {
    private String line;
    private final String name;
    private long lineNumber = 0;
    private boolean hasNext = true;
    private LineNumberReader fileReader;
    private final TupleFactory tupleFactory;


    public FileIterator(FileReader fileReader, TupleFactory tupleFactory, String name) {
        this.name = name;
        this.tupleFactory = tupleFactory;
        this.fileReader = new LineNumberReader(fileReader);
    }

    @Override
    public boolean hasNext() {
        try {
            if (!this.hasNext) {
                return false;
            }

            if (this.line == null) {
                this.line = this.fileReader.readLine();
                lineNumber++;

                if (this.line == null) {
                    this.hasNext = false;
                    this.fileReader.close();
                    this.fileReader = null;
                    return false;
                }
            }

            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple<String, String> next() {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator closed");
        }

        try {
            return this.tupleFactory.tuple(this.name + lineNumber, this.line);
        } finally {
            this.line = null;
        }
    }
}