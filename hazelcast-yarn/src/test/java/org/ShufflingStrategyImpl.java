package org;

import com.hazelcast.nio.Address;
import com.hazelcast.yarn.api.ShufflingStrategy;
import com.hazelcast.yarn.api.container.ContainerContext;

import java.net.UnknownHostException;

public class ShufflingStrategyImpl implements ShufflingStrategy {
    private final int port;
    private final String host;

    private transient volatile Address address;

    public ShufflingStrategyImpl(Address address) {
        this.host = address.getHost();
        this.port = address.getPort();
    }

    @Override
    public Address getShufflingAddress(ContainerContext containerContext) {
        if (address == null) {
            try {
                address = new Address(host, port);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return address;
    }
}