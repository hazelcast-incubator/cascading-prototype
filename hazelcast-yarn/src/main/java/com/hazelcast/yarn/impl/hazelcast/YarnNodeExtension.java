package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.instance.DefaultNodeExtension;

public class YarnNodeExtension extends DefaultNodeExtension {
    @Override
    public ReadHandler createReadHandler(TcpIpConnection connection, IOService ioService) {
        NodeEngineImpl nodeEngine = node.nodeEngine;
        return new YarnReadHandler(connection, nodeEngine.getPacketDispatcher());
    }
}
