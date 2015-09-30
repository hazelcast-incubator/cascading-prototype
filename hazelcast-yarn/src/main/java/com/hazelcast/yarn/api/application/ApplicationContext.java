package com.hazelcast.yarn.api.application;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.DAG;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.config.YarnApplicationConfig;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.api.statemachine.ApplicationStateMachine;
import com.hazelcast.yarn.api.application.localization.LocalizationStorage;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

/***
 * ApplicationContext which represents context of the certain application.
 * Used during calculation process
 */
public interface ApplicationContext {
    /***
     * @return direct acyclic graph corresponding to application
     */
    DAG getDAG();

    /***
     * @return name of the application
     */
    String getName();

    /***
     * @return node's address which created application
     */
    Address getOwner();

    /***
     * @return node engine of corresponding to the current node
     */
    NodeEngine getNodeEngine();

    AtomicInteger getContainerIDGenerator();

    ApplicationMaster getApplicationMaster();

    ApplicationExecutor getProcessingExecutor();

    boolean validateOwner(Address applicationOwner);

    LocalizationStorage getLocalizationStorage();

    ApplicationExecutor getTapStateMachineExecutor();

    YarnApplicationConfig getYarnApplicationConfig();

    ApplicationStateMachine getApplicationStateMachine();

    ApplicationExecutor getApplicationStateMachineExecutor();

    ApplicationExecutor getTupleContainerStateMachineExecutor();

    ApplicationExecutor getApplicationMasterStateMachineExecutor();

    void registerApplicationListener(ApplicationListener applicationListener);

    List<ApplicationListener> getListeners();
}
