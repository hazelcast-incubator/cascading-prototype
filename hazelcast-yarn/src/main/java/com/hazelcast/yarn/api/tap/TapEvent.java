package com.hazelcast.yarn.api.tap;

import com.hazelcast.yarn.api.statemachine.StateMachineEvent;

public enum TapEvent implements StateMachineEvent {
    OPEN,
    CLOSE
}
