package com.hazelcast.yarn.impl.application.localization.classloader;

import java.io.InputStream;

public class ResourceStream {
    private final String baseUrl;
    private final InputStream inputStream;

    public ResourceStream(InputStream inputStream) {
        this.inputStream = inputStream;
        this.baseUrl = null;
    }

    public ResourceStream(InputStream inputStream, String baseUrl) {
        this.inputStream = inputStream;
        this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public InputStream getInputStream() {
        return inputStream;
    }
}
