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

import java.util.Iterator;

import com.hazelcast.config.Config;
import com.hazelcast.instance.Node;

import java.lang.reflect.Constructor;

import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceImpl;

public class JetNode extends Node {
    private static final String FACTORY_ID = "com.hazelcast.jet.api.hazelcast.JetNodeExtension";

    public JetNode(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        super(hazelcastInstance, config, nodeContext);
    }

    public NodeExtension create(NodeContext nodeContext) {
        try {
            ClassLoader classLoader = this.getConfigClassLoader();
            Iterator<Class<NodeExtension>> iter = ServiceLoader.classIterator(FACTORY_ID, classLoader);
            while (iter.hasNext()) {
                Class<NodeExtension> clazz = iter.next();
                if (!(clazz.equals(DefaultNodeExtension.class))) {
                    Constructor<NodeExtension> constructor = clazz
                            .getDeclaredConstructor(new Class[]{Node.class});
                    return constructor.newInstance(this);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return new JetNodeExtension(this, super.createNodeExtension(nodeContext));
    }

    protected NodeExtension createNodeExtension(NodeContext nodeContext) {
        if (getBuildInfo().isEnterprise()) {
            return create(nodeContext);
        } else {
            return new JetNodeExtension(this, super.createNodeExtension(nodeContext));
        }
    }
}
