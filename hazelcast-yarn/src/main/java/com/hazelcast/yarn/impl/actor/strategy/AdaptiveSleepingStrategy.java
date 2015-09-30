package com.hazelcast.yarn.impl.actor.strategy;

import java.util.concurrent.locks.LockSupport;

import com.hazelcast.yarn.api.actor.SleepingStrategy;

public class AdaptiveSleepingStrategy implements SleepingStrategy {
    private static final int MAX_POWER_OF_TWO = 10;

    private int power = 1;

    private static long getDurationNanos(int power) {
        return (1L << power) * 1000L;
    }

    private long nextSleepingDurationNanos(boolean loaded) {
        if (loaded) {
            power = 1;
        } else {
            if (power < MAX_POWER_OF_TWO) {
                power++;
            }
        }

        return getDurationNanos(power);
    }

    @Override
    public void await(boolean loaded) {
        LockSupport.parkNanos(nextSleepingDurationNanos(loaded));
    }
}
