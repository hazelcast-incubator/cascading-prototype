package com.hazelcast.yarn.impl.application;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.DAG;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.config.YarnApplicationConfig;

import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.impl.executor.StateMachineExecutor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.impl.container.ApplicationMasterImpl;
import com.hazelcast.yarn.impl.executor.DefaultApplicationExecutor;
import com.hazelcast.yarn.api.statemachine.ApplicationStateMachine;
import com.hazelcast.yarn.api.application.localization.LocalizationType;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;
import com.hazelcast.yarn.api.application.localization.LocalizationStorage;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.impl.application.localization.LocalizationStorageFactory;


import com.hazelcast.yarn.impl.statemachine.application.ApplicationStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.application.ApplicationStateMachineFactory;
import com.hazelcast.yarn.impl.statemachine.application.DefaultApplicationStateMachineRequestProcessor;

public class ApplicationContextImpl implements ApplicationContext {
    private final String name;
    private final NodeEngine nodeEngine;
    private final YarnApplicationConfig config;
    private final AtomicReference<Address> owner;
    private final AtomicInteger containerIdGenerator;
    private final ApplicationMaster applicationMaster;
    private final ApplicationExecutor processingExecutor;
    private final LocalizationStorage localizationStorage;
    private final YarnApplicationConfig yarnApplicationConfig;
    private final ApplicationStateMachine applicationStateMachine;
    private final ApplicationExecutor applicationStateMachineExecutor;
    private final ApplicationExecutor applicationMasterStateMachineExecutor;
    private final ApplicationExecutor containerStateMachineExecutor;
    private final ApplicationExecutor tapStateMachineExecutor;

    private static final ApplicationStateMachineFactory STATE_MACHINE_FACTORY = new ApplicationStateMachineFactory() {
        @Override
        public ApplicationStateMachine newStateMachine(String name, StateMachineRequestProcessor<ApplicationEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext) {
            return new ApplicationStateMachineImpl(name, processor, nodeEngine, applicationContext);
        }
    };

    public ApplicationContextImpl(String name,
                                  NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.owner = new AtomicReference<Address>();
        this.containerIdGenerator = new AtomicInteger(0);
        this.yarnApplicationConfig = nodeEngine.getConfig().getYarnApplicationConfig(name);
        this.processingExecutor = new DefaultApplicationExecutor(this.name + "-application_executor", this.yarnApplicationConfig.getMaxProcessingThreads(), nodeEngine);
        this.applicationStateMachineExecutor = new StateMachineExecutor(this.name + "-application-state_machine", 1, nodeEngine);
        this.applicationMasterStateMachineExecutor = new StateMachineExecutor(this.name + "-application-master-state_machine", 1, nodeEngine);
        this.containerStateMachineExecutor = new StateMachineExecutor(this.name + "-container-state_machine", 10, nodeEngine);
        this.tapStateMachineExecutor = new StateMachineExecutor(this.name + "-tap-state_machine", 1, nodeEngine);

        this.localizationStorage = LocalizationStorageFactory.getLocalizationStorage(
                LocalizationType.valueOf(this.nodeEngine.getConfig().getYarnApplicationConfig(name).getLocalizationType()),
                this.nodeEngine.getConfig(),
                name
        );

        this.applicationStateMachine = STATE_MACHINE_FACTORY.newStateMachine(
                name,
                new DefaultApplicationStateMachineRequestProcessor(),
                nodeEngine,
                this
        );

        this.applicationMaster = new ApplicationMasterImpl(
                this
        );

        this.config = nodeEngine.getConfig().getYarnApplicationConfig(this.name);
    }

    @Override
    public boolean validateOwner(Address applicationOwner) {
        return this.owner.compareAndSet(null, applicationOwner) || (this.owner.compareAndSet(applicationOwner, applicationOwner));
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Address getOwner() {
        return this.owner.get();
    }

    @Override
    public LocalizationStorage getLocalizationStorage() {
        return this.localizationStorage;
    }

    @Override
    public ApplicationStateMachine getApplicationStateMachine() {
        return this.applicationStateMachine;
    }

    public ApplicationMaster getApplicationMaster() {
        return applicationMaster;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public YarnApplicationConfig getYarnApplicationConfig() {
        return this.config;
    }

    @Override
    public ApplicationExecutor getApplicationStateMachineExecutor() {
        return this.applicationStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getApplicationMasterStateMachineExecutor() {
        return this.applicationMasterStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getTupleContainerStateMachineExecutor() {
        return this.containerStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getTapStateMachineExecutor() {
        return this.tapStateMachineExecutor;
    }

    @Override
    public ApplicationExecutor getProcessingExecutor() {
        return processingExecutor;
    }

    @Override
    public AtomicInteger getContainerIDGenerator() {
        return containerIdGenerator;
    }

    @Override
    public DAG getDAG() {
        return applicationMaster.getDag();
    }
}
