package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.yarn.api.application.Initable;
import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.yarn.api.application.Application;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

public class YarnHazelcastIntanceImpl extends HazelcastInstanceImpl implements YarnHazelcastInstance {
    public YarnHazelcastIntanceImpl(String name, Config config, NodeContext nodeContext) throws Exception {
        super(name, config, nodeContext);
    }

    protected Node createNode(Config config, NodeContext nodeContext) {
        return new YarnNode(this, config, nodeContext);
    }

    @Override
    public Application getYarnApplication(String applicationName) {
        checkNotNull(applicationName, "Retrieving an application instance with a null name is not allowed!");
        checkTrue(applicationName.length() <= 32, "Length of applicationName shouldn't be greater than 32");

        Application application = getDistributedObject(YarnService.SERVICE_NAME, applicationName);
        ((Initable) application).init();

        return application;
    }
}
