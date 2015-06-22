package com.hazelcast.yarn.impl.application.localization;

import java.util.Map;
import java.io.IOException;
import java.util.LinkedHashMap;

import com.hazelcast.config.Config;
import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.yarn.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.yarn.api.application.localization.LocalizationStorage;
import com.hazelcast.yarn.api.application.localization.InvalidLocalizationException;
import com.hazelcast.yarn.impl.application.localization.classloader.ApplicationClassLoader;
import com.hazelcast.yarn.impl.application.localization.classloader.ResourceStream;

public abstract class AbstractLocalizationStorage<S> implements LocalizationStorage {
    private final Config config;

    private ClassLoader classLoader;

    private volatile boolean accepted = false;

    protected final Map<LocalizationResourceDescriptor, S> resources = new LinkedHashMap<LocalizationResourceDescriptor, S>();

    public AbstractLocalizationStorage(Config config) {
        this.config = config;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public abstract ResourceStream asResourceStream(S resource) throws IOException;

    public Map<LocalizationResourceDescriptor, ResourceStream> getResources() throws IOException {
        Map<LocalizationResourceDescriptor, ResourceStream> resourceStreams = new LinkedHashMap<LocalizationResourceDescriptor, ResourceStream>(resources.size());

        for (Map.Entry<LocalizationResourceDescriptor, S> entry : resources.entrySet()) {
            resourceStreams.put(entry.getKey(), asResourceStream(entry.getValue()));
        }

        return resourceStreams;
    }

    public void receiveFileChunk(Chunk chunk) {
        LocalizationResourceDescriptor descriptor = chunk.getDescriptor();
        S resource = getResource(resources.get(descriptor), chunk);
        resources.put(descriptor, resource);
    }

    public Config getConfig() {
        return config;
    }

    protected abstract S getResource(S resource, Chunk chunk);

    public void accept() throws InvalidLocalizationException {
        if (accepted)
            return;

        this.accepted = true;
        this.classLoader = new ApplicationClassLoader(this);
    }
}
