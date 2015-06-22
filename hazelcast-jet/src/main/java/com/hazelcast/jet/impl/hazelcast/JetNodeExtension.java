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

package com.hazelcast.jet.impl.hazelcast;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.IOService;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.internal.serialization.SerializationService;

public class JetNodeExtension implements NodeExtension {
    private final Node node;
    private JetService jetService;
    private final NodeExtension delegate;

    public JetNodeExtension(Node node, NodeExtension delegate) {
        this.node = node;
        this.delegate = delegate;
    }

    @Override
    public MessageTaskFactory createMessageTaskFactory() {
        List<MessageTaskFactory> list = new ArrayList<MessageTaskFactory>();
        list.add(new com.hazelcast.client.impl.protocol.MessageTaskFactoryImpl(this.node));
        list.add(new com.hazelcast.client.impl.protocol.JetMessageTaskFactoryImpl(this.node));
        return new JetMessageTaskFactoryImpl(list, this.node);
    }

    @Override
    public void onThreadStart(Thread thread) {
        this.delegate.onThreadStart(thread);
    }

    @Override
    public void onThreadStop(Thread thread) {
        this.delegate.onThreadStop(thread);
    }

    @Override
    public MemoryStats getMemoryStats() {
        return this.delegate.getMemoryStats();
    }

    @Override
    public void beforeShutdown() {
        this.delegate.beforeShutdown();
    }

    @Override
    public void shutdown() {
        this.delegate.shutdown();
    }

    @Override
    public void validateJoinRequest() {
        this.delegate.validateJoinRequest();
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean persistentChange) {
        this.delegate.onClusterStateChange(newState, persistentChange);
    }

    @Override
    public boolean registerListener(Object listener) {
        return this.delegate.registerListener(listener);
    }

    @Override
    public boolean triggerForceStart() {
        return this.delegate.triggerForceStart();
    }

    @Override
    public void beforeStart() {
        this.delegate.beforeStart();
    }

    @Override
    public void printNodeInfo() {
        this.delegate.printNodeInfo();
    }

    @Override
    public void beforeJoin() {
        this.delegate.beforeJoin();
    }

    @Override
    public void afterStart() {
        this.delegate.afterStart();
    }

    @Override
    public boolean isStartCompleted() {
        return this.delegate.isStartCompleted();
    }

    @Override
    public SerializationService createSerializationService() {
        return this.delegate.createSerializationService();
    }

    @Override
    public SecurityContext getSecurityContext() {
        return this.delegate.getSecurityContext();
    }

    @Override
    public <T> T createService(Class<T> type) {
        return this.delegate.createService(type);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        Map<String, Object> services = delegate.createExtensionServices();

        if (this.jetService == null) {
            this.jetService = new JetServiceImpl(node.getNodeEngine());
        }

        if (services.isEmpty()) {
            services = Collections.<String, Object>singletonMap(JetService.SERVICE_NAME, this.jetService);
        } else {
            services.put(JetService.SERVICE_NAME, this.jetService);
        }

        return services;
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return this.delegate.getMemberSocketInterceptor();
    }

    @Override
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        return this.delegate.getSocketChannelWrapperFactory();
    }

    @Override
    public ReadHandler createReadHandler(TcpIpConnection connection, IOService ioService) {
        return this.delegate.createReadHandler(connection, ioService);
    }

    @Override
    public WriteHandler createWriteHandler(TcpIpConnection connection, IOService ioService) {
        return this.delegate.createWriteHandler(connection, ioService);
    }
}
