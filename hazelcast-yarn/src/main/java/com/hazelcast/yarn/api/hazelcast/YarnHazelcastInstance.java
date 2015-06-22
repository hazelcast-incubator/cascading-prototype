package com.hazelcast.yarn.api.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.yarn.api.application.Application;

public interface YarnHazelcastInstance extends HazelcastInstance {
    /**
     * Returns an application with the specified name.
     *
     * @param applicationName
     * @return distributed Yarn application
     */
    Application getYarnApplication(String applicationName);
}
