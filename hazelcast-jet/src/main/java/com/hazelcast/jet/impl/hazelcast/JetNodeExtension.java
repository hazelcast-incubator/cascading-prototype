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

import java.util.List;
import java.util.ArrayList;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;

public class JetNodeExtension extends DefaultNodeExtension {
    public JetNodeExtension(Node node) {
        super(node);
    }

    @Override
    public ReadHandler createReadHandler(TcpIpConnection connection, IOService ioService) {
        return new JetReadHandler(connection, this.node.nodeEngine.getPacketDispatcher());
    }

    @Override
    public List<? extends MessageTaskFactory> createMessageTaskFactories() {
        List<MessageTaskFactory> list = new ArrayList<MessageTaskFactory>();
        list.add(new com.hazelcast.client.impl.protocol.MessageTaskFactoryImpl(this.node));
        list.add(new com.hazelcast.client.impl.protocol.JetMessageTaskFactoryImpl(this.node));
        return list;
    }
}
