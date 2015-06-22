package com.hazelcast.yarn.api;

import java.io.Serializable;

import com.hazelcast.nio.Address;
import com.hazelcast.yarn.api.container.ContainerContext;

public interface ShufflingStrategy extends Serializable {
    Address getShufflingAddress(ContainerContext containerContext);
}
