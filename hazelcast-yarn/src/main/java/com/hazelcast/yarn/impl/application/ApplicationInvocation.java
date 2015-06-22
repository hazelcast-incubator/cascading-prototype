package com.hazelcast.yarn.impl.application;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.yarn.api.hazelcast.YarnService;

public class ApplicationInvocation implements Runnable {
    private final Operation operation;
    private final Address address;
    private final YarnService yarnService;
    private final NodeEngine nodeEngine;

    public ApplicationInvocation(Operation operation, Address address, YarnService yarnService, NodeEngine nodeEngine) {
        this.operation = operation;
        this.address = address;
        this.yarnService = yarnService;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void run() {
        this.executeOperation(operation, address, yarnService, nodeEngine);
    }

    private <V> V executeOperation(Operation operation, Address address, YarnService yarnService,
                                   NodeEngine nodeEngine) {

        ClusterService cs = nodeEngine.getClusterService();
        OperationService os = nodeEngine.getOperationService();
        boolean returnsResponse = operation.returnsResponse();

        try {
            if (cs.getThisAddress().equals(address)) {
                // Locally we can call the operation directly
                operation.setNodeEngine(nodeEngine);
                operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
                operation.setService(yarnService);
                operation.run();

                if (returnsResponse) {
                    return (V) operation.getResponse();
                }
            } else {
                if (returnsResponse) {
                    InvocationBuilder ib = os.createInvocationBuilder(YarnService.SERVICE_NAME, operation, address);
                    return (V) ib.invoke().get();
                } else {
                    os.send(operation, address);
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return null;
    }

}
