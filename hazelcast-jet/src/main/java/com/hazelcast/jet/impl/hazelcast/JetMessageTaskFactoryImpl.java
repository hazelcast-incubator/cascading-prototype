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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.task.NoSuchMessageTask;

public class JetMessageTaskFactoryImpl implements MessageTaskFactory {
    private final Node node;
    private final List<MessageTaskFactory> factories;

    public JetMessageTaskFactoryImpl(List<MessageTaskFactory> subFactories,
                                     Node node) {
        this.node = node;
        this.factories = subFactories;
    }

    @Override
    public MessageTask create(ClientMessage clientMessage, Connection connection) {
        MessageTask task = null;

        for (int idx = 0; idx < this.factories.size(); idx++) {
            MessageTaskFactory messageTaskFactory = this.factories.get(idx);
            task = messageTaskFactory.create(clientMessage, connection);
            if (!(task instanceof NoSuchMessageTask)) {
                return task;
            }
        }

        return task == null ? new NoSuchMessageTask(clientMessage, this.node, connection) : task;
    }
}
