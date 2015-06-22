/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.instance;

import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.ServiceLoader;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

@SuppressWarnings("SynchronizationOnStaticField")
public final class HazelcastInstanceFactory {
    public static final IHazelcastInstanceFactory INSTANCE;

    static {
        ServiceLoader serviceLoader = ServiceLoader.load(IHazelcastInstanceFactory.class);
        Iterator iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            INSTANCE = ServiceLoader.load(IHazelcastInstanceFactory.class).iterator().next();
        } else {
            INSTANCE = new DefaultHazelcastInstanceFactory();
        }
    }

    private HazelcastInstanceFactory() {
    }

    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return INSTANCE.getAllHazelcastInstances();
    }

    public static HazelcastInstance getHazelcastInstance(String instanceName) {
        return INSTANCE.getHazelcastInstance(instanceName);
    }

    public static HazelcastInstance getOrCreateHazelcastInstance(Config config) {
        return INSTANCE.getOrCreateHazelcastInstance(config);
    }

    public static HazelcastInstance newHazelcastInstance(Config config) {
        return INSTANCE.newHazelcastInstance(config);
    }

    /**
     * Creates a new Hazelcast instance.
     *
     * @param config       the configuration to use; if <code>null</code>, the set of defaults
     *                     as specified in the XSD for the configuration XML will be used.
     * @param instanceName
     * @param nodeContext
     * @return
     */
    public static HazelcastInstance newHazelcastInstance(Config config, String instanceName,
                                                         NodeContext nodeContext) {
        return INSTANCE.newHazelcastInstance(config, instanceName, nodeContext);
    }

    public static void remove(HazelcastInstanceImpl instance) {
        INSTANCE.remove(instance);
    }

    public static void shutdownAll() {
        INSTANCE.shutdownAll();
    }

    public static void terminateAll() {
        INSTANCE.terminateAll();
    }

    public static Map<MemberImpl, HazelcastInstanceImpl> getInstanceImplMap() {
        return INSTANCE.getInstanceImplMap();
    }
}