package com.hazelcast.yarn.impl.container;

import com.hazelcast.yarn.impl.SettableFuture;
import com.hazelcast.yarn.api.statemachine.StateMachineEvent;
import com.hazelcast.yarn.api.statemachine.StateMachineOutput;

public class RequestPayLoad<SI extends StateMachineEvent, SO extends StateMachineOutput> {
    private final SettableFuture<SO> future;
    private final Object payLoad;
    private final SI event;

    public RequestPayLoad(SI event, Object payLoad) {
        this.event = event;
        this.payLoad = payLoad;
        this.future = SettableFuture.create();
    }

    public SettableFuture<SO> getFuture() {
        return future;
    }

    public SI getEvent() {
        return event;
    }

    public Object getPayLoad() {
        return payLoad;
    }
}
