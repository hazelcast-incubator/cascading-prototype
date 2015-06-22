package com.hazelcast.yarn.impl.operation.application;


public class InitApplicationRequestOperation extends AbstractYarnApplicationRequestOperation {
    public InitApplicationRequestOperation() {
    }

    public InitApplicationRequestOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        resolveApplicationContext();
    }
}
