package com.hazelcast.yarn.api.application;

public interface ApplicationListener {
    void onApplicationExecuted(ApplicationContext applicationContext);
}
