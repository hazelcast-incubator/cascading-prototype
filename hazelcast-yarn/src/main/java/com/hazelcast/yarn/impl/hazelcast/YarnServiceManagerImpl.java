package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;

public class YarnServiceManagerImpl extends ServiceManagerImpl {
    public YarnServiceManagerImpl(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        this.registerService(
                YarnService.SERVICE_NAME,
                new YarnServiceImpl(this.nodeEngine)
        );
    }
}
