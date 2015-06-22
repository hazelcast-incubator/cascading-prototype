package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import java.util.Iterator;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.statemachine.container.requests.ExecuteTupleContainerRequest;

public class ExecuteApplicationProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final long secondsToAwait;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;

    public ExecuteApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        NodeEngine nodeEngine = applicationMaster.getNodeEngine();
        this.applicationContext = applicationMaster.getApplicationContext();
        YarnApplicationConfig config = nodeEngine.getConfig().getYarnApplicationConfig(applicationContext.getName());
        this.secondsToAwait = config.getApplicationSecondsToAwait();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.applicationMaster.registerExecution();

        Iterator<Vertex> iterator = this.applicationMaster.getDag().getRevertedTopologicalVertexIterator();

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            TupleContainer tupleContainer = this.applicationMaster.getContainerByVertex(vertex);
            tupleContainer.handleContainerRequest(new ExecuteTupleContainerRequest()).get(this.secondsToAwait, TimeUnit.SECONDS);
            Object result = this.applicationMaster.synchronizeWithOtherNodes(tupleContainer).poll(this.secondsToAwait, TimeUnit.SECONDS);

            if ((result != null) && (result instanceof Throwable)) {
                throw new RuntimeException((Throwable) result);
            }
        }

        this.applicationContext.getProcessingExecutor().execute();
    }
}