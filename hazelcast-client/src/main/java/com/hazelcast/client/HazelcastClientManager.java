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

package com.hazelcast.client;

import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;

import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.ConcurrentMap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.DefaultClientConnectionManagerFactory;

/***
 * This is just abstract manager for the clients in the system
 */
public class HazelcastClientManager {
    /***
     * Singleton instance for the HazelcastClientManager
     */
    public static final HazelcastClientManager HAZELCAST_CLIENT_MANAGER = new HazelcastClientManager();

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private static final ConcurrentMap<String, HazelcastClientProxy> CLIENTS
            = new ConcurrentHashMap<String, HazelcastClientProxy>(5);

    public HazelcastInstance newHazelcastClient(HazelcastClientFactory hazelcastClientFactory) {
        return newHazelcastClient(new XmlClientConfigBuilder().build(), hazelcastClientFactory);
    }

    public HazelcastInstance newHazelcastClient(ClientConfig config, HazelcastClientFactory hazelcastClientFactory) {
        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            ClientConnectionManagerFactory clientConnectionManagerFactory = new DefaultClientConnectionManagerFactory();
            final HazelcastClientInstanceImpl client = hazelcastClientFactory.createHazelcastInstanceClient(
                    config,
                    clientConnectionManagerFactory
            );
            client.start();
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = hazelcastClientFactory.createProxy(client);

            if (CLIENTS.containsKey(client.getName())) {
                throw new DuplicateInstanceNameException("HazelcastClientInstance with name '" + client.getName()
                        + "' already exists!");
            }
            CLIENTS.put(client.getName(), proxy);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    public HazelcastInstance getHazelcastClientByName(String instanceName) {
        return CLIENTS.get(instanceName);
    }

    public Collection<HazelcastInstance> getAllHazelcastClients() {
        Collection<HazelcastClientProxy> values = CLIENTS.values();
        return Collections.unmodifiableCollection(new HashSet<HazelcastInstance>(values));
    }

    public void shutdownAll() {
        for (HazelcastClientProxy proxy : CLIENTS.values()) {
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                continue;
            }
            proxy.client = null;
            try {
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        OutOfMemoryErrorDispatcher.clearClients();
        CLIENTS.clear();
    }


    public void shutdown(HazelcastInstance instance) {
        if (instance instanceof HazelcastClientProxy) {
            final HazelcastClientProxy proxy = (HazelcastClientProxy) instance;
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                return;
            }
            proxy.client = null;
            CLIENTS.remove(client.getName());

            try {
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                OutOfMemoryErrorDispatcher.deregisterClient(client);
            }
        }
    }

    public void shutdown(String instanceName) {
        HazelcastClientProxy proxy = CLIENTS.remove(instanceName);
        if (proxy == null) {
            return;
        }
        HazelcastClientInstanceImpl client = proxy.client;
        if (client == null) {
            return;
        }
        proxy.client = null;
        try {
            client.shutdown();
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            OutOfMemoryErrorDispatcher.deregisterClient(client);
        }
    }

    public void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setClientHandler(outOfMemoryHandler);
    }
}
