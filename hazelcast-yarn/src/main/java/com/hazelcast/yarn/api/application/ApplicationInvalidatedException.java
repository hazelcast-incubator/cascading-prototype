package com.hazelcast.yarn.api.application;

import com.hazelcast.nio.Address;
import com.hazelcast.yarn.impl.hazelcast.YarnPacket;

public class ApplicationInvalidatedException extends RuntimeException {
    private final String initiatorAddress;
    private final YarnPacket wrongPacket;
    private final Object reason;

    public ApplicationInvalidatedException(Object reason, Address initiator) {
        this.reason = reason;
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = null;
    }

    public ApplicationInvalidatedException(Address initiator) {
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = null;
        this.reason = null;
    }

    public ApplicationInvalidatedException(Address initiator, YarnPacket wrongPacket) {
        this.initiatorAddress = initiator != null ? initiator.toString() : "";
        this.wrongPacket = wrongPacket;
        this.reason = null;
    }

    public String toString() {
        if (this.initiatorAddress != null) {
            String error = "Application was invalidated by member with address: " + initiatorAddress;

            if (this.wrongPacket == null) {
                return error;
            } else {
                return error + " wrongPacket=" + wrongPacket.toString();
            }
        } else if (this.reason != null) {
            return "Application was invalidated because of : " + this.reason.toString();
        } else {
            return "Application was invalidated for unknown reason";
        }
    }
}
