package com.hazelcast.yarn.impl.application.localization.classloader;

public class ClassLoaderEntry {
    private final byte[] resourceBytes;
    private final String baseUrl;

    public ClassLoaderEntry(byte[] resourceBytes, String baseUrl) {
        this.resourceBytes = resourceBytes;
        this.baseUrl = baseUrl;
    }

    public byte[] getResourceBytes() {
        return resourceBytes;
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
