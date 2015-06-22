package com.hazelcast.yarn.impl;

import java.util.Set;
import java.util.Iterator;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.util.NoSuchElementException;

import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.impl.application.LocalizationResource;

public final class ChunkIterator implements Iterator<Chunk> {
    private long currentLength;
    private final int chunkSize;
    private InputStream currentInputStream;
    private LocalizationResource currentURL;
    private final Iterator<LocalizationResource> urlIterator;

    public ChunkIterator(Set<LocalizationResource> urls, YarnApplicationConfig yarnApplicationConfig) {
        this.urlIterator = urls.iterator();
        this.chunkSize = yarnApplicationConfig.getResourceFileChunkSize();
    }

    private void switchFile() throws IOException {
        if (!this.urlIterator.hasNext())
            throw new NoSuchElementException();

        this.currentURL = this.urlIterator.next();
        this.currentInputStream = this.currentURL.openStream();
        this.currentLength = this.currentInputStream.available();
    }

    @Override
    public boolean hasNext() {
        try {
            return (this.urlIterator.hasNext()) || (
                    (this.currentInputStream != null)
                            &&
                            (this.currentInputStream.available() > 0)

            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new IllegalStateException();
    }

    @Override
    public Chunk next() {
        try {
            if (this.currentInputStream == null) {
                do {
                    this.switchFile();
                } while (this.currentInputStream.available() <= 0);
            }

            if (this.currentInputStream.available() <= 0) {
                throw new NoSuchElementException();
            }

            byte[] bytes = YarnUtil.readChunk(this.currentInputStream, this.chunkSize);

            if (this.currentInputStream.available() <= 0) {
                this.close(this.currentInputStream);
                this.currentInputStream = null;
            }

            if (bytes.length > 0) {
                return new Chunk(
                        bytes,
                        this.currentURL.getDescriptor(),
                        this.currentLength
                );
            } else {
                throw new NoSuchElementException();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void close(InputStream inputStream) throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
