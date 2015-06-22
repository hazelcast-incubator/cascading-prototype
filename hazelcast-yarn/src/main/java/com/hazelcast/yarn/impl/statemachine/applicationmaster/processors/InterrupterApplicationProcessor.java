package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.yarn.api.Dummy;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.statemachine.container.requests.InterruptContainerRequest;

public class InterrupterApplicationProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final int secondToAwait;
    private final ApplicationMaster applicationMaster;

    public InterrupterApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.secondToAwait = applicationMaster.getNodeEngine().getConfig().getYarnApplicationConfig(applicationMaster.getApplicationContext().getName()).getYarnSecondsToAwait();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.applicationMaster.registerInterruption();

        try {
            for (TupleContainer container : this.applicationMaster.containers()) {
                container.handleContainerRequest(new InterruptContainerRequest()).get(this.secondToAwait, TimeUnit.SECONDS);
            }
        } finally {
            this.applicationMaster.getApplicationContext().getProcessingExecutor().interrupt();
        }
    }
}
