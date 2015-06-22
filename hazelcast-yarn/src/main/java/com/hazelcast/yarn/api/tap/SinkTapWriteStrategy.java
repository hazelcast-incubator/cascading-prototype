package com.hazelcast.yarn.api.tap;

public enum SinkTapWriteStrategy implements TapStrategy {
    CLEAR_AND_REPLACE,
    APPEND,
}
