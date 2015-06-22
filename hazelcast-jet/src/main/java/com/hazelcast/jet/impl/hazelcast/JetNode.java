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

import com.hazelcast.config.Config;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.instance.HazelcastInstanceImpl;

public class JetNode extends Node {
    public JetNode(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        super(hazelcastInstance, config, nodeContext);
    }

    protected NodeExtension createNodeExtension(NodeContext nodeContext) {
        return new JetNodeExtension(this);
    }

    protected NodeEngineImpl createNodeEngine() {
        return new JetNodeEngineImpl(this);
    }
}
