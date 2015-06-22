package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.concurrent.ConcurrentMap;

import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.yarn.api.YarnApplicationManager;
import com.hazelcast.yarn.api.application.ApplicationProxy;
import com.hazelcast.yarn.impl.application.ApplicationProxyImpl;
import com.hazelcast.yarn.impl.container.DefaultYarnPackedHandler;
import com.hazelcast.yarn.impl.application.YarnApplicationManagerImpl;


public class YarnServiceImpl implements YarnService {
    private final NodeEngine nodeEngine;
    private final PacketHandler packetHandler;
    private final YarnApplicationManager applicationManager;
    private final ConcurrentMap<String, ApplicationProxy> applications;

    private final ConstructorFunction<String, ApplicationProxy> constructor = new ConstructorFunction<String, ApplicationProxy>() {
        @Override
        public ApplicationProxy createNew(String name) {
            return new ApplicationProxyImpl(name, YarnServiceImpl.this, nodeEngine);
        }
    };

    public YarnServiceImpl(NodeEngine nodeEngine) {
        this.applicationManager = new YarnApplicationManagerImpl(nodeEngine);
        this.packetHandler = new DefaultYarnPackedHandler(this.applicationManager, nodeEngine);
        this.nodeEngine = nodeEngine;
        this.applications = new ConcurrentHashMap<String, ApplicationProxy>();
    }

    @Override
    public YarnApplicationManager getApplicationManager() {
        return this.applicationManager;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        this.packetHandler.handle(packet);
    }

    @Override
    public ApplicationProxy createDistributedObject(String objectName) {
        return ConcurrencyUtil.getOrPutSynchronized(this.applications, objectName, this.applications, this.constructor);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        ApplicationProxy applicationProxy = this.applications.get(objectName);

        if (applicationProxy != null) {
            applicationProxy.destroy();
        }
    }
}