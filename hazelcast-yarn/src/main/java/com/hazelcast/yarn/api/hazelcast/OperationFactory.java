package com.hazelcast.yarn.api.hazelcast;

import com.hazelcast.spi.Operation;

public interface OperationFactory {
    Operation operation();
}
