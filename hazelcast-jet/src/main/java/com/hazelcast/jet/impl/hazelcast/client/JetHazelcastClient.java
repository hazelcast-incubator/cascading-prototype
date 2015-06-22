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

package com.hazelcast.jet.impl.hazelcast.client;

import java.util.Collection;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClientFactory;
import com.hazelcast.client.HazelcastClientManager;
import com.hazelcast.jet.api.config.JetClientConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;

public final class JetHazelcastClient {
    private static final HazelcastClientManager HAZELCAST_CLIENT_MANAGER = HazelcastClientManager.INSTANCE;

    private static final HazelcastClientFactory<JetHazelcastClientInstanceImpl, JetHazelcastClientProxy, JetClientConfig>
            HAZELCAST_CLIENT_FACTORY =
            new HazelcastClientFactory<JetHazelcastClientInstanceImpl, JetHazelcastClientProxy, JetClientConfig>() {
                @Override
                public JetHazelcastClientInstanceImpl createHazelcastInstanceClient(JetClientConfig config,
                                                                                    ClientConnectionManagerFactory factory) {
                    return new JetHazelcastClientInstanceImpl(config, factory, null);
                }

                @Override
                public JetHazelcastClientProxy createProxy(JetHazelcastClientInstanceImpl client) {
                    return new JetHazelcastClientProxy(client);
                }
            };

    private JetHazelcastClient() {
    }

    public static JetHazelcastInstance newHazelcastClient() {
        return (JetHazelcastInstance) HAZELCAST_CLIENT_MANAGER.newHazelcastClient(HAZELCAST_CLIENT_FACTORY);
    }

    public static JetHazelcastInstance newHazelcastClient(ClientConfig config) {
        return (JetHazelcastInstance) HAZELCAST_CLIENT_MANAGER.newHazelcastClient(config, HAZELCAST_CLIENT_FACTORY);
    }

    /**
     * Returns an existing HazelcastClient with instanceName.
     *
     * @param instanceName Name of the HazelcastInstance (client) which can be retrieved by {@link HazelcastInstance#getName()}
     * @return HazelcastInstance
     */
    public static JetHazelcastInstance getHazelcastClientByName(String instanceName) {
        return (JetHazelcastInstance) HAZELCAST_CLIENT_MANAGER.getHazelcastClientByName(instanceName);
    }

    /**
     * Gets an immutable collection of all client HazelcastInstances created in this JVM.
     * <p/>
     * In managed environments such as Java EE or OSGi Hazelcast can be loaded by multiple classloaders. Typically you will get
     * at least one classloader per every application deployed. In these cases only the client HazelcastInstances created
     * by the same application will be seen, and instances created by different applications are invisible.
     * <p/>
     * The returned collection is a snapshot of the client HazelcastInstances. So changes to the client HazelcastInstances
     * will not be visible in this collection.
     *
     * @return the collection of client HazelcastInstances
     */
    public static Collection<HazelcastInstance> getAllHazelcastClients() {
        return HAZELCAST_CLIENT_MANAGER.getAllHazelcastClients();
    }

    /**
     * Shuts down all the client HazelcastInstance created in this JVM.
     * <p/>
     * To be more precise it shuts down the HazelcastInstances loaded using the same classloader this HazelcastClient has been
     * loaded with.
     * <p/>
     * This method is mostly used for testing purposes.
     *
     * @see #getAllHazelcastClients()
     */
    public static void shutdownAll() {
        HAZELCAST_CLIENT_MANAGER.shutdownAll();
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instance the hazelcast client instance
     */
    public static void shutdown(HazelcastInstance instance) {
        HAZELCAST_CLIENT_MANAGER.shutdown(instance);
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instanceName the hazelcast client instance name
     */
    public static void shutdown(String instanceName) {
        HAZELCAST_CLIENT_MANAGER.shutdown(instanceName);
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hazelcast Client threads.
     * <p/>
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <tt>OutOfMemoryError</tt> is caught by HazelcastClient threads
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        HAZELCAST_CLIENT_MANAGER.setOutOfMemoryHandler(outOfMemoryHandler);
    }
}
