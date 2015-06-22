/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.hazelcast;

import java.nio.ByteBuffer;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.MemberReadHandler;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

public class JetReadHandler extends MemberReadHandler {
    public JetReadHandler(TcpIpConnection connection,
                          PacketDispatcher packetTransceiver) {
        super(connection, packetTransceiver);
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            if (packet == null) {
                byte version = src.array()[src.position()];
                if (version == JetPacket.VERSION) {
                    packet = new JetPacket();
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
