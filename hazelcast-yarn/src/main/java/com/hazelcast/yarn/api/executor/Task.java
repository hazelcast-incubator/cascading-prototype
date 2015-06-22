package com.hazelcast.yarn.api.executor;


public interface Task {
    boolean executeTask(Payload payload);
}
