package com.hazelcast.yarn.api.dag;

import java.io.Serializable;

import com.hazelcast.yarn.api.ShufflingStrategy;

public interface Edge extends Serializable {
    String getName();

    Vertex getInputVertex();

    Vertex getOutputVertex();

    boolean isShuffled();

    ShufflingStrategy getShufflingStrategy();
}
