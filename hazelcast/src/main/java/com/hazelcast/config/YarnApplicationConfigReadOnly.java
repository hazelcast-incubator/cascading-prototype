package com.hazelcast.config;


public class YarnApplicationConfigReadOnly extends YarnApplicationConfig {
    public YarnApplicationConfigReadOnly(YarnApplicationConfig defConfig, String name) {
        super(defConfig, name);
    }

    public YarnApplicationConfigReadOnly(String name) {
        super(name);
    }

    public void setLocalizationType(String localizationType) {
        throw new IllegalStateException("ReadOnly");
    }

    public void setResourceFileChunkSize(byte resourceFileChunkSize) {
        throw new IllegalStateException("ReadOnly");
    }

    public void setLocalizationDirectory(String localizationDirectory) {
        throw new IllegalStateException("ReadOnly");
    }

}
