package com.hazelcast.yarn.impl.hazelcast;

import java.nio.ByteBuffer;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.MemberReadHandler;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

public class YarnReadHandler extends MemberReadHandler {
    public YarnReadHandler(TcpIpConnection connection,
                           PacketDispatcher packetTransceiver) {
        super(connection, packetTransceiver);
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            if (packet == null) {
                byte version = src.array()[src.position()];
                if (version == YarnPacket.VERSION) {
                    packet = new YarnPacket();
                } else if (version == Packet.VERSION) {
                    packet = new Packet();
                } else {
                    throw new IllegalStateException("Wrong version");
                }
            }

            boolean complete = packet.readFrom(src);

            if (complete) {
                try {
                    handlePacket(packet);
                } finally {
                    packet = null;
                }
            } else {
                break;
            }
        }
    }

}
