package com.hazelcast.yarn.api.application.localization;

import java.util.Map;
import java.io.IOException;

import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.yarn.api.YarnException;
import com.hazelcast.yarn.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.yarn.impl.application.localization.classloader.ResourceStream;


public interface LocalizationStorage {
    void receiveFileChunk(Chunk chunk) throws IOException, YarnException;

    void accept() throws InvalidLocalizationException;

    ClassLoader getClassLoader();

    Map<LocalizationResourceDescriptor, ResourceStream> getResources() throws IOException;
}
