package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

public class ApplicationFinalizerProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final ApplicationMaster applicationMaster;

    public ApplicationFinalizerProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void process(Dummy payload) throws Exception {
        Throwable error = null;

        for (TupleContainer container : this.applicationMaster.containers()) {
            try {
                container.destroy();
            } catch (Throwable e) {
                error = e;
            }
        }

        if (error != null) {
            throw new RuntimeException(error);
        }
    }
}
