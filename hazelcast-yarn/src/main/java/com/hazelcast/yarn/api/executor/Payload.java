package com.hazelcast.yarn.api.executor;

public interface Payload {
    void set(boolean produced);

    boolean produced();
}
