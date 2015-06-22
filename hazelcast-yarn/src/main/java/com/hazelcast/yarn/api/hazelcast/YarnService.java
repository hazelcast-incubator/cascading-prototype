package com.hazelcast.yarn.api.hazelcast;

import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.yarn.api.YarnApplicationManager;

public interface YarnService extends PacketHandler, RemoteService {
    String SERVICE_NAME = "hz:impl:yarnService";

    YarnApplicationManager getApplicationManager();
}
