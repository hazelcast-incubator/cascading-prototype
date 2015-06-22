package com.hazelcast.yarn.api.container;

public interface ContainerPayLoadProcessor<PayLoad> {
    void process(PayLoad payload) throws Exception;
}
