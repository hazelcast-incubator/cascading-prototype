package com.hazelcast.yarn.impl.actor.ringbuffer;

abstract class LeftPaddedLong {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RightPaddedLong extends LeftPaddedLong {
    private volatile long value;

    RightPaddedLong(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}

public class PaddedLong extends RightPaddedLong {
    protected long p1, p2, p3, p4, p5, p6, p7;

    public PaddedLong(long value) {
        super(value);
    }
}
