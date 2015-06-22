package com.hazelcast.instance;

import java.util.Map;
import java.util.Set;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

public interface IHazelcastInstanceFactory {
    Set<HazelcastInstance> getAllHazelcastInstances();

    HazelcastInstance getHazelcastInstance(String instanceName);

    HazelcastInstance getOrCreateHazelcastInstance(Config config);

    HazelcastInstance newHazelcastInstance(Config config);

    HazelcastInstance newHazelcastInstance(Config config, String instanceName,
                                           NodeContext nodeContext);

    void remove(HazelcastInstanceImpl instance);

    void shutdownAll();

    void terminateAll();

    Map<MemberImpl, HazelcastInstanceImpl> getInstanceImplMap();
}
