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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Packet;

public final class JetPacket extends Packet {
    public static final byte VERSION = 105;
    public static final int HEADER_JET_DATA_CHUNK = 10;
    public static final int HEADER_JET_DATA_CHUNK_SENT = 11;
    public static final int HEADER_JET_SHUFFLER_CLOSED = 12;
    public static final int HEADER_JET_DATA_NO_APP_FAILURE = 13;
    public static final int HEADER_JET_DATA_NO_CONTAINER_FAILURE = 14;
    public static final int HEADER_JET_DATA_NO_TASK_FAILURE = 15;
    public static final int HEADER_JET_DATA_NO_MEMBER_FAILURE = 16;
    public static final int HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE = 17;
    public static final int HEADER_JET_UNKNOWN_EXCEPTION_FAILURE = 18;
    public static final int HEADER_JET_APPLICATION_IS_NOT_EXECUTING = 19;
    public static final int HEADER_JET_CONTAINER_STARTED = 20;
    public static final int HEADER_JET_INVALIDATE_APPLICATION = 21;
    public static final int HEADER_JET_MEMBER_EVENT = 22;

    private static final short PERSIST_TASK_ID = 10;
    private static final short PERSIST_CONTAINER = 11;
    private static final short PERSIST_APPLICATION_SIZE = 12;
    private static final short PERSIST_APPLICATION = 13;

    private int taskID;
    private int containerId;
    private Address remoteMember;
    private byte[] applicationNameBytes;

    public JetPacket() {

    }

    public JetPacket(byte[] applicationNameBytes, byte[] payLoad) {
        this(-1, -1, applicationNameBytes, payLoad);
    }


    public JetPacket(byte[] applicationNameBytes) {
        this(-1, -1, applicationNameBytes, null);
    }

    public JetPacket(int containerId,
                     byte[] applicationNameBytes
    ) {
        this(-1, containerId, applicationNameBytes, null);
    }

    public JetPacket(int taskID,
                     int containerId,
                     byte[] applicationNameBytes
    ) {
        this(taskID, containerId, applicationNameBytes, null);
    }

    public JetPacket(int taskID,
                     int containerId,
                     byte[] applicationNameBytes,
                     byte[] payLoad
    ) {
        super(payLoad, -1);

        this.taskID = taskID;
        this.containerId = containerId;
        this.applicationNameBytes = applicationNameBytes;
    }

    public void setHeader(int bit) {
        this.header = (short) bit;
    }

    public boolean isHeaderSet(int bit) {
        return this.header != 0;
    }

    //CHECKSTYLE:OFF
    public boolean writeTo(ByteBuffer destination) {
        if (!writeVersion(destination)) {
            return false;
        }

        if (!writeHeader(destination)) {
            return false;
        }

        if (!writePartition(destination)) {
            return false;
        }

        if (!writeSize(destination)) {
            return false;
        }

        if (!writeValue(destination)) {
            return false;
        }

        if (!writeTask(destination)) {
            return false;
        }

        if (!writeContainer(destination)) {
            return false;
        }

        if (!writeApplicationNameBytesSize(destination)) {
            return false;
        }

        if (!writeApplicationNameBytes(destination)) {
            return false;
        }

        return true;
    }
    //CHECKSTYLE:ON

    private boolean writeApplicationNameBytesSize(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (destination.remaining() < this.applicationNameBytes.length) {
                return false;
            }

            destination.putInt(this.applicationNameBytes.length);
            setPersistStatus(PERSIST_APPLICATION_SIZE);
        }
        return true;
    }

    //CHECKSTYLE:OFF
    public boolean readFrom(ByteBuffer source) {
        if (!readVersion(source)) {
            return false;
        }

        if (!readHeader(source)) {
            return false;
        }

        if (!readPartition(source)) {
            return false;
        }

        if (!readSize(source)) {
            return false;
        }

        if (!readValue(source)) {
            return false;
        }

        if (!readTask(source)) {
            return false;
        }

        if (!readContainer(source)) {
            return false;
        }

        if (!readApplicationNameBytesSize(source)) {
            return false;
        }

        if (!readApplicationNameBytes(source)) {
            return false;
        }

        return true;
    }
    //CHECKSTYLE:ON



    // ========================= Task =================================================
    private boolean writeTask(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_TASK_ID)) {
            if (destination.remaining() <= Bits.INT_SIZE_IN_BYTES) {
                return false;
            }

            destination.putInt(this.taskID);
            setPersistStatus(PERSIST_TASK_ID);
        }
        return true;
    }

    private boolean readTask(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_TASK_ID)) {
            if (source.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }
            this.taskID = source.getInt();
            setPersistStatus(PERSIST_TASK_ID);
        }
        return true;
    }

    // ========================= container =================================================

    private boolean writeContainer(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_CONTAINER)) {
            if (destination.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }

            destination.putInt(this.containerId);
            setPersistStatus(PERSIST_CONTAINER);
        }

        return true;
    }

    private boolean readContainer(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_CONTAINER)) {
            if (source.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }

            this.containerId = source.getInt();
            setPersistStatus(PERSIST_CONTAINER);
        }

        return true;
    }


    // ========================= application ==========================
    private boolean writeApplicationNameBytes(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (this.applicationNameBytes.length > 0) {
                if (destination.remaining() < this.applicationNameBytes.length) {
                    return false;
                }

                destination.put(this.applicationNameBytes);
            }

            setPersistStatus(PERSIST_APPLICATION);
        }
        return true;
    }

    private boolean readApplicationNameBytesSize(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION_SIZE)) {
            if (source.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }

            int size = source.getInt();
            this.applicationNameBytes = new byte[size];
            setPersistStatus(PERSIST_APPLICATION_SIZE);
        }

        return true;
    }


    private boolean readApplicationNameBytes(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_APPLICATION)) {
            if (source.remaining() < this.applicationNameBytes.length) {
                return false;
            }

            source.get(this.applicationNameBytes);

            setPersistStatus(PERSIST_APPLICATION);
        }
        return true;
    }

    // ========================= Getters =========================

    protected boolean readVersion(ByteBuffer source) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!source.hasRemaining()) {
                return false;
            }

            byte version = source.get();
            setPersistStatus(PERSIST_VERSION);
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }
        }
        return true;
    }

    protected boolean writeVersion(ByteBuffer destination) {
        if (!isPersistStatusSet(PERSIST_VERSION)) {
            if (!destination.hasRemaining()) {
                return false;
            }
            destination.put(VERSION);
            setPersistStatus(PERSIST_VERSION);
        }
        return true;
    }

    public int getTaskID() {
        return this.taskID;
    }

    public int getContainerId() {
        return this.containerId;
    }

    public byte[] getApplicationNameBytes() {
        return this.applicationNameBytes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JetPacket{").append("header=").
                append(header).
                append(", isResponse=").append(isHeaderSet(Packet.HEADER_RESPONSE)).
                append(", isOperation=").append(isHeaderSet(Packet.HEADER_OP)).
                append(", isEvent=").append(isHeaderSet(Packet.HEADER_EVENT)).
                append(", containerId=").append(this.containerId).
                append(", applicationName=").
                append(
                        this.applicationNameBytes == null
                                ?
                                "null"
                                :
                                new String(this.applicationNameBytes)
                ).
                append(", taskID=").append(this.taskID).
                append(", conn=").append(this.conn == null ? "null" : this.conn).
                append('}');

        return sb.toString();
    }

    public Address getRemoteMember() {
        return remoteMember;
    }

    public void setRemoteMember(Address remoteMember) {
        this.remoteMember = remoteMember;
    }
}
