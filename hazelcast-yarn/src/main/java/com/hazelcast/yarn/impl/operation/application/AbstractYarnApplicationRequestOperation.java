package com.hazelcast.yarn.impl.operation.application;

import java.io.IOException;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.yarn.api.YarnException;
import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.yarn.api.YarnApplicationManager;
import com.hazelcast.yarn.api.application.ApplicationContext;

public abstract class AbstractYarnApplicationRequestOperation extends AbstractOperation {
    protected String name;
    private Object result = true;

    protected AbstractYarnApplicationRequestOperation() {
    }

    protected AbstractYarnApplicationRequestOperation(String name) {
        this.name = name;
    }

    public boolean returnsResponse() {
        return true;
    }

    public String getName() {
        return this.name;
    }

    protected ApplicationContext resolveApplicationContext() throws YarnException {
        YarnService yarnService = getService();
        Address applicationOwner = getCallerAddress();

        if (applicationOwner == null) {
            applicationOwner = getNodeEngine().getThisAddress();
        }

        YarnApplicationManager yarnApplicationManager = yarnService.getApplicationManager();
        ApplicationContext applicationContext = yarnApplicationManager.createOrGetApplicationContext(this.name);

        if (!applicationContext.validateOwner(applicationOwner)) {
            throw new YarnException("Invalid applicationOwner for applicationId " + this.name + " applicationOwner=" + applicationOwner + " Current=" + applicationContext.getOwner());
        }

        return applicationContext;
    }

    public Object getResponse() {
        return this.result;
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(this.name);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.name = in.readObject();
    }
}
