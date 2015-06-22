package com.hazelcast.yarn.api;

import com.hazelcast.yarn.api.application.ApplicationContext;


public interface YarnApplicationManager {
    ApplicationContext createOrGetApplicationContext(String name);

    ApplicationContext getApplicationContext(String name);

    void destroyApplication(String name);
}
