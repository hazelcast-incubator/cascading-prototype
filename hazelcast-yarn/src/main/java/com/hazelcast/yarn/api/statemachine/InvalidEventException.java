package com.hazelcast.yarn.api.statemachine;

public class InvalidEventException extends Exception {
    private final Object event;
    private final Object state;

    public Object getState() {
        return state;
    }

    public Object getEvent() {
        return event;
    }

    public InvalidEventException(Object event, Object state, String name) {
        super("Invalid Event " + event + " for state=" + state + " for stateMachine with name=" + name);
        this.event = event;
        this.state = state;
    }
}
