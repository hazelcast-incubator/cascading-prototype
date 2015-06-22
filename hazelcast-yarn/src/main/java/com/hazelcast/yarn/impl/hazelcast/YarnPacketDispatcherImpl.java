package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.nio.Packet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.spi.impl.packetdispatcher.impl.PacketDispatcherImpl;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

public class YarnPacketDispatcherImpl extends PacketDispatcherImpl {
    private final YarnService yarnService;

    public YarnPacketDispatcherImpl(ILogger logger,
                                    PacketHandler operationPacketHandler,
                                    PacketHandler eventPacketHandler,
                                    PacketHandler wanReplicationPacketHandler,
                                    PacketHandler connectionPacketHandler,
                                    YarnService yarnService) {
        super(logger, operationPacketHandler, eventPacketHandler, wanReplicationPacketHandler, connectionPacketHandler);
        this.yarnService = yarnService;
    }

    @Override
    public void dispatch(Packet packet) {
        try {
            if (packet instanceof YarnPacket) {
                YarnPacket yarnPacket = (YarnPacket) packet;
                yarnService.handle(yarnPacket);
            } else {
                super.dispatch(packet);
            }
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe("Failed to process packet:" + packet, t);
        }
    }
}
