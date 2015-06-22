package com.hazelcast.yarn.impl.operation.application;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.BuildExecutionPlanRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.NotifyExecutionPlanReadyRequest;

public class SubmitApplicationRequestOperation extends AbstractYarnApplicationRequestOperation {
    private DAG dag;

    public SubmitApplicationRequestOperation() {
        super();
    }

    public SubmitApplicationRequestOperation(String name, DAG dag) {
        super(name);
        this.dag = dag;
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        dag.validate();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();


        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(new BuildExecutionPlanRequest(dag));
        long secondsToAwait = this.getNodeEngine().getConfig().getYarnApplicationConfig(getName()).getApplicationSecondsToAwait();

        try {
            ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

            if (response != ApplicationMasterResponse.SUCCESS)
                throw new IllegalStateException("Unable to submit dag");

            applicationMaster.handleContainerRequest(new NotifyExecutionPlanReadyRequest()).get(secondsToAwait, TimeUnit.SECONDS);
            applicationMaster.setDag(dag);

            if (response != ApplicationMasterResponse.SUCCESS)
                throw new IllegalStateException("Unable to submit dag");
        } catch (Throwable e) {
            this.getLogger().warning(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(dag);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        dag = in.readObject();
    }
}
