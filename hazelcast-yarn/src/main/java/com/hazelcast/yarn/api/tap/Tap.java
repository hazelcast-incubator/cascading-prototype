package com.hazelcast.yarn.api.tap;

import java.io.Serializable;

public interface Tap extends Serializable {
    String getName();

    TapType getType();

    boolean isSource();

    boolean isSink();
}
