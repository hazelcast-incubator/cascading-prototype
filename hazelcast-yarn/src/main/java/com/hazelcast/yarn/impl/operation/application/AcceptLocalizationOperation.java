package com.hazelcast.yarn.impl.operation.application;

import com.hazelcast.yarn.api.application.ApplicationContext;

public class AcceptLocalizationOperation extends AbstractYarnApplicationRequestOperation {
    public AcceptLocalizationOperation() {}

    public AcceptLocalizationOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        applicationContext.getLocalizationStorage().accept();
    }
}
