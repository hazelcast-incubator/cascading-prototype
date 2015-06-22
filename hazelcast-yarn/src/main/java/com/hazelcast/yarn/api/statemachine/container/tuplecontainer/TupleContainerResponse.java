package com.hazelcast.yarn.api.statemachine.container.tuplecontainer;

import com.hazelcast.yarn.api.container.ContainerResponse;

public interface TupleContainerResponse extends ContainerResponse {
    TupleContainerResponse SUCCESS = new TupleContainerResponse() {
    };
    TupleContainerResponse FAILURE = new TupleContainerResponse() {
    };

}
