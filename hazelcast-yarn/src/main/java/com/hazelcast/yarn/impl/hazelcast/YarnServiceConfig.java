package com.hazelcast.yarn.impl.hazelcast;

import com.hazelcast.config.ServiceConfig;
import com.hazelcast.yarn.api.hazelcast.YarnService;

import java.util.Properties;

public class YarnServiceConfig extends ServiceConfig {
    public static final String YARN_RECEIVING_QUEUE_SIZE_PROP_NAME = "hazelcast.yarn.receiving.queue.size";
    public static final String YARN_RECEIVING_QUEUE_SIZE = String.valueOf(100000);

    public YarnServiceConfig() {
        Properties properties = new Properties();
        properties.setProperty(YARN_RECEIVING_QUEUE_SIZE_PROP_NAME, YARN_RECEIVING_QUEUE_SIZE);
        this.setProperties(properties);
    }

    public String getName() {
        return YarnService.SERVICE_NAME;
    }
}
