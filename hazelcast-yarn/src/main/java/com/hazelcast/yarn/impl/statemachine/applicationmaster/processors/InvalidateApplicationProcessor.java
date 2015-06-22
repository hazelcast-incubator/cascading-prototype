package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;


public class InvalidateApplicationProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final long secondsToAwait;
    private final NodeEngine nodeEngine;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;

    public InvalidateApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.nodeEngine = applicationMaster.getNodeEngine();
        this.applicationContext = applicationMaster.getApplicationContext();
        YarnApplicationConfig config = this.nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName());
        this.secondsToAwait = config.getApplicationSecondsToAwait();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        for (TupleContainer container : applicationMaster.containers()) {
            container.invalidate();
        }
    }
}