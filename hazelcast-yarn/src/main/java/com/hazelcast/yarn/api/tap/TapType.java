package com.hazelcast.yarn.api.tap;

import java.io.Serializable;

public enum TapType implements Serializable {
    HAZELCAST_LIST,
    HAZELCAST_MAP,
    HAZELCAST_MULTIMAP,
    FILE,
    HD_FILE,
    OTHER
}
