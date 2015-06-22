package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.DefaultHazelcastInstanceFactory;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;

public class YarnHazelcastInstanceFactory extends DefaultHazelcastInstanceFactory {
    public YarnHazelcastIntanceImpl newHazelcastInstance(Config config, String instanceName,
                                                         NodeContext nodeContext) {
        try {
            return new YarnHazelcastIntanceImpl(getInstanceName(instanceName, config), config, nodeContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected HazelcastInstanceProxy newHazelcastProxy(HazelcastInstanceImpl hazelcastInstance) {
        return new YarnHazelcastInstanceProxy(hazelcastInstance);
    }

    public YarnHazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        ServiceConfig myServiceConfig = new YarnServiceConfig();
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(myServiceConfig);

        return (YarnHazelcastInstance) super.newHazelcastInstance(config);
    }
}
