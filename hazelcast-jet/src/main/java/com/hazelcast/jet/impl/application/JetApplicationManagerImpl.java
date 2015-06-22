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

package com.hazelcast.jet.impl.application;

import java.io.IOException;
import java.util.Collection;

import com.hazelcast.nio.Address;

import java.net.InetSocketAddress;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.core.IFunction;

import java.net.StandardSocketOptions;

import com.hazelcast.util.IConcurrentMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;

import java.nio.channels.ServerSocketChannel;

import com.hazelcast.jet.api.JetApplicationManager;
import com.hazelcast.util.SampleableConcurrentHashMap;
import com.hazelcast.jet.impl.executor.NetworkExecutor;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.executor.ApplicationExecutor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.container.task.nio.DefaultSocketThreadAcceptor;

public class JetApplicationManagerImpl implements JetApplicationManager {
    private final NodeEngine nodeEngine;
    private final Address localJetAddress;
    private final ServerSocketChannel serverSocketChannel;

    private final ThreadLocal<JetApplicationConfig> threadLocal = new ThreadLocal<JetApplicationConfig>();

    private final IConcurrentMap<String, ApplicationContext> applicationContexts =
            new SampleableConcurrentHashMap<String, ApplicationContext>(16);

    private final IFunction<String, ApplicationContext> function = new IFunction<String, ApplicationContext>() {
        @Override
        public ApplicationContext apply(String name) {
            return new ApplicationContextImpl(
                    name,
                    nodeEngine,
                    localJetAddress,
                    threadLocal.get()
            );
        }
    };

    public JetApplicationManagerImpl(final NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            String host = nodeEngine.getLocalMember().getAddress().getHost();
            int port = bindSocketChannel(this.serverSocketChannel, host);
            this.localJetAddress = new Address(host, port);

            final ApplicationExecutor applicationExecutor = new NetworkExecutor(
                    "network-acceptor",
                    1,
                    Integer.MAX_VALUE,
                    nodeEngine
            );

            applicationExecutor.addDistributed(
                    new DefaultSocketThreadAcceptor(this, nodeEngine, this.serverSocketChannel)
            );

            applicationExecutor.startWorkers();
            applicationExecutor.execute();

            nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(
                    new LifecycleListener() {
                        @Override
                        public void stateChanged(LifecycleEvent event) {
                            if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                                try {
                                    applicationExecutor.shutdown();
                                } catch (Exception e) {
                                    nodeEngine.getLogger(getClass()).warning(e.getMessage(), e);
                                }
                            }
                        }
                    }
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //CHECKSTYLE:OFF
    private int bindSocketChannel(ServerSocketChannel serverSocketChannel, String host) {
        try {
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            int port = JetApplicationConfig.DEFAULT_PORT;

            while (port <= 0xFFFF) {
                try {
                    this.serverSocketChannel.bind(new InetSocketAddress(host, port));
                    break;
                } catch (java.nio.channels.AlreadyBoundException e) {
                    port += JetApplicationConfig.PORT_AUTO_INCREMENT;
                } catch (java.net.BindException e) {
                    port += JetApplicationConfig.PORT_AUTO_INCREMENT;
                }
            }

            this.serverSocketChannel.configureBlocking(false);
            return port;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //CHECKSTYLE:ON

    @Override
    public ApplicationContext createOrGetApplicationContext(String name, JetApplicationConfig config) {
        this.threadLocal.set(config);

        try {
            return this.applicationContexts.applyIfAbsent(
                    name,
                    this.function
            );
        } finally {
            this.threadLocal.set(null);
        }
    }

    @Override
    public ApplicationContext getApplicationContext(String name) {
        return this.applicationContexts.get(name);
    }

    @Override
    public void destroyApplication(String name) {
        this.applicationContexts.remove(name);
    }

    @Override
    public Address getLocalJetAddress() {
        return this.localJetAddress;
    }

    @Override
    public Collection<ApplicationContext> getApplicationContexts() {
        return this.applicationContexts.values();
    }
}
