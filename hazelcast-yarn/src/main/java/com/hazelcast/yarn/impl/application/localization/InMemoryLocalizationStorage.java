package com.hazelcast.yarn.impl.application.localization;

import java.io.IOException;

import com.hazelcast.config.Config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.yarn.impl.application.localization.classloader.ResourceStream;

public class InMemoryLocalizationStorage extends AbstractLocalizationStorage<ByteArrayOutputStream> {
    public InMemoryLocalizationStorage(Config config) {
        super(config);
    }

    @Override
    public ResourceStream asResourceStream(ByteArrayOutputStream resource) throws IOException {
        return new ResourceStream(
                new ByteArrayInputStream(
                        resource.toByteArray()
                )
        );
    }

    @Override
    protected ByteArrayOutputStream getResource(ByteArrayOutputStream resource, Chunk chunk) {
        try {
            if (resource == null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(chunk.getBytes());
                return baos;
            } else {
                resource.write(chunk.getBytes());
                return resource;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
