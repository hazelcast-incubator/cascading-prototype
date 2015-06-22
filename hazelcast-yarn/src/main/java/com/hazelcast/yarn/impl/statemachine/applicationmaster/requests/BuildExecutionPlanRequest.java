package com.hazelcast.yarn.impl.statemachine.applicationmaster.requests;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class BuildExecutionPlanRequest implements ContainerRequest<ApplicationMasterEvent, DAG> {
    private final DAG dag;

    public BuildExecutionPlanRequest(DAG dag) {
        this.dag = dag;
    }

    @Override
    public ApplicationMasterEvent getContainerEvent() {
        return ApplicationMasterEvent.SUBMIT_DAG;
    }

    @Override
    public DAG getPayLoad() {
        return this.dag;
    }
}
