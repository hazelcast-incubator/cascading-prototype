package com.hazelcast.client;

import com.hazelcast.client.spi.ClientProxy;

public class ClientYarnProxy extends ClientProxy {
    protected ClientYarnProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }
}
