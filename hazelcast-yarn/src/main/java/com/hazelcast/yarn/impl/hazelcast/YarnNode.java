package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.instance.HazelcastInstanceImpl;

public class YarnNode extends Node {
    public YarnNode(HazelcastInstanceImpl hazelcastInstance, Config config, NodeContext nodeContext) {
        super(hazelcastInstance, config, nodeContext);
    }

    protected NodeExtension createNodeExtension() {
        return new YarnNodeExtension();
    }

    protected NodeEngineImpl createNodeEngine() {
        return new YarnNodeEngineImpl(this);
    }
}
