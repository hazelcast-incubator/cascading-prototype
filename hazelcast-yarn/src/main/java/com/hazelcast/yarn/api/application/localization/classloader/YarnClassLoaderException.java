package com.hazelcast.yarn.api.application.localization.classloader;

import java.io.IOException;

public class YarnClassLoaderException extends RuntimeException {
    public YarnClassLoaderException(String message) {
        super(message);
    }

    public YarnClassLoaderException(IOException e) {
        super(e);
    }
}
