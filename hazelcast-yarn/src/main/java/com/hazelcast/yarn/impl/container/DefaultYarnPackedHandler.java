package com.hazelcast.yarn.impl.container;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.impl.hazelcast.YarnPacket;
import com.hazelcast.yarn.api.YarnApplicationManager;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class DefaultYarnPackedHandler implements PacketHandler {
    private final NodeEngineImpl nodeEngine;
    private final YarnApplicationManager applicationManager;

    public DefaultYarnPackedHandler(YarnApplicationManager applicationManager, NodeEngine nodeEngine) {
        this.applicationManager = applicationManager;
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        YarnPacket yarnPacket = (YarnPacket) packet;

        String applicationName = new String(yarnPacket.getApplicationNameBytes());

        ApplicationContext applicationContext = this.applicationManager.getApplicationContext(
                applicationName
        );

        if (applicationContext == null) {
            sendResponse(yarnPacket, YarnPacket.HEADER_YARN_TUPLE_NO_APP_FAILURE);
        } else {
            int result = applicationContext.getApplicationMaster().receiveYarnPacket(yarnPacket);

            if (result != 0) {
                sendResponse(yarnPacket, result);
            }
        }
    }

    private void sendResponse(YarnPacket yarnPacket, int header) {
        yarnPacket.reset();
        yarnPacket.setHeader(header);
        this.nodeEngine.getNode().getConnectionManager().transmit(yarnPacket, yarnPacket.getConn().getEndPoint());
    }
}
