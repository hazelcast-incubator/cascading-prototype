package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

public class YarnHazelcastInstanceProxy extends HazelcastInstanceProxy implements YarnHazelcastInstance {
    protected YarnHazelcastInstanceProxy(HazelcastInstanceImpl original) {
        super(original);
    }

    public YarnHazelcastIntanceImpl getOriginal() {
        final YarnHazelcastIntanceImpl hazelcastInstance = (YarnHazelcastIntanceImpl) original;

        if (hazelcastInstance == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return hazelcastInstance;
    }

    @Override
    public Application getYarnApplication(String applicationName) {
        return getOriginal().getYarnApplication(applicationName);
    }
}
