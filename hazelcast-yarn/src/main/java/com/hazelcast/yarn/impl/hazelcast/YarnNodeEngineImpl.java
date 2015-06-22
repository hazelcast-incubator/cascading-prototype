package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;
import com.hazelcast.spi.impl.servicemanager.impl.ServiceManagerImpl;

public class YarnNodeEngineImpl extends NodeEngineImpl {
    public YarnNodeEngineImpl(Node node) {
        super(node);
    }

    protected ServiceManagerImpl createServiceManager() {
        return new YarnServiceManagerImpl(this);
    }

    protected PacketDispatcher createPacketDispatcher() {
        return new YarnPacketDispatcherImpl(
                logger,
                operationService,
                eventService,
                wanReplicationService,
                connectionManagerPacketHandler(),
                this.serviceManager.<YarnService>getService(YarnService.SERVICE_NAME)
        );
    }
}
