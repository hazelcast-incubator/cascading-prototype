/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.container;


import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;

import com.hazelcast.nio.Address;
import com.hazelcast.config.Config;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.instance.MemberImpl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.CombinedJetException;
import com.hazelcast.jet.impl.hazelcast.JetPacket;

import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.DiscoveryService;
import com.hazelcast.jet.api.executor.ApplicationExecutor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ExecutionErrorHolder;
import com.hazelcast.jet.api.application.ApplicationListener;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.jet.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.jet.api.application.ApplicationInvalidatedException;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;


import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.InvalidateApplicationRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.processors.ApplicationMasterPayLoadFactory;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterStateMachineFactory;

public class ApplicationMasterImpl extends
        AbstractServiceContainer<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
        implements ApplicationMaster {

    private static final InterruptedException APPLICATION_INTERRUPTED_EXCEPTION =
            new InterruptedException("Application has been interrupted");

    private static final ApplicationMasterStateMachineFactory STATE_MACHINE_FACTORY =
            new DefaultApplicationMasterStateMachineFactory();

    private final AtomicBoolean invalidated = new AtomicBoolean(false);

    private final List<DataContainer> containers = new CopyOnWriteArrayList<DataContainer>();
    private final Map<Integer, DataContainer> containersCache = new ConcurrentHashMap<Integer, DataContainer>();

    private final Map<Vertex, DataContainer> vertex2ContainerCache = new ConcurrentHashMap<Vertex, DataContainer>();

    private final AtomicInteger executedContainers = new AtomicInteger(0);
    private final AtomicInteger interruptedContainers = new AtomicInteger(0);

    private final AtomicReference<BlockingQueue<Object>> executionMailBox =
            new AtomicReference<BlockingQueue<Object>>(null);

    private final AtomicReference<BlockingQueue<Object>> interruptionFutureHolder =
            new AtomicReference<BlockingQueue<Object>>(null);

    private volatile DAG dag;

    private final ConcurrentMap<Integer, BlockingQueue<Object>> registrations =
            new ConcurrentHashMap<Integer, BlockingQueue<Object>>();

    private final ConcurrentMap<Integer, AtomicInteger> registrationCounters =
            new ConcurrentHashMap<Integer, AtomicInteger>();


    private final ApplicationExecutor networkExecutor;

    private final DiscoveryService discoveryService;

    public ApplicationMasterImpl(
            ApplicationContext applicationContext,
            ApplicationExecutor networkExecutor,
            DiscoveryService discoveryService

    ) {
        super(STATE_MACHINE_FACTORY, applicationContext.getNodeEngine(), applicationContext);
        this.networkExecutor = networkExecutor;
        this.discoveryService = discoveryService;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void processRequest(ApplicationMasterEvent event, Object payLoad) throws Exception {
        ContainerPayLoadProcessor processor = ApplicationMasterPayLoadFactory.getProcessor(event, this);

        if (processor != null) {
            processor.process(payLoad);
        }
    }

    public void startNetWorkEngine() {
        this.networkExecutor.startWorkers();
        this.networkExecutor.execute();
    }

    @Override
    public void handleContainerInterrupted(DataContainer tapContainer) {
        if (this.interruptedContainers.incrementAndGet() >= containers.size()) {
            handleContainerRequest(new ExecutionInterruptedRequest());

            this.interruptedContainers.set(0);
            this.executedContainers.set(this.containers.size());
            this.interruptionFutureHolder.get().offer(ApplicationMasterResponse.SUCCESS);

            addToExecutionMailBox(APPLICATION_INTERRUPTED_EXCEPTION);
        }
    }

    @Override
    public void handleContainerCompleted(DataContainer container) {
        if (this.executedContainers.decrementAndGet() <= 0) {
            this.executedContainers.set(containers.size());
            handleContainerRequest(new ExecutionCompletedRequest());
        }
    }

    @Override
    public void registerExecution() {
        this.executionMailBox.set(new LinkedBlockingQueue<Object>());
        this.executedContainers.set(this.containers.size());
    }

    @Override
    public void registerInterruption() {
        this.interruptionFutureHolder.set(new LinkedBlockingQueue<Object>());
        this.interruptedContainers.set(0);
    }

    @Override
    public BlockingQueue<Object> getExecutionMailBox() {
        return this.executionMailBox.get();
    }

    @Override
    public BlockingQueue<Object> getInterruptionMailBox() {
        return this.interruptionFutureHolder.get();
    }

    @Override
    public void setExecutionError(ExecutionErrorHolder executionError) {
        this.executedContainers.set(containers.size());
    }

    @Override
    public List<DataContainer> containers() {
        return Collections.unmodifiableList(this.containers);
    }

    @Override
    public void addToExecutionMailBox(Object object) {
        List<ApplicationListener> listeners = getApplicationContext().getApplicationListeners();
        List<Throwable> errors = new ArrayList<Throwable>(listeners.size());

        System.out.println("addToExecutionMailBox.1");

        try {

            for (ApplicationListener listener : listeners) {
                try {
                    listener.onApplicationExecuted(getApplicationContext());
                } catch (Throwable e) {
                    errors.add(e);
                }
            }

        } finally {
            BlockingQueue<Object> executionMailBox = this.executionMailBox.get();


            if (executionMailBox != null) {
                if (errors.size() > 0) {
                    if ((object != null) && (object instanceof Throwable)) {
                        errors.add((Throwable) object);
                    }

                    CombinedJetException exception = new CombinedJetException(errors);
                    executionMailBox.offer(exception);
                } else {
                    executionMailBox.offer(object);
                }
            }
        }

        System.out.println("addToExecutionMailBox.2");
    }

    public void invalidateApplicationLocal(Object reason) {
        if (this.invalidated.compareAndSet(false, true)) {
            Throwable error = getError(reason);
            handleContainerRequest(new InvalidateApplicationRequest(error));
        }
    }

    private Throwable getError(Object reason) {
        Throwable error;
        if (reason instanceof JetPacket) {
            JetPacket packet = (JetPacket) reason;
            Address initiator = packet.getRemoteMember();

            error = packet.toByteArray() == null
                    ?
                    new ApplicationInvalidatedException(initiator)
                    :
                    toException(initiator, (JetPacket) reason);
        } else if (reason instanceof Address) {
            error = new ApplicationInvalidatedException((Address) reason);
        } else if (reason instanceof Throwable) {
            error = new CombinedJetException(
                    Arrays.asList(
                            (Throwable) reason,
                            new ApplicationInvalidatedException(getNodeEngine().getLocalMember().getAddress())
                    )
            );
        } else {
            error = new ApplicationInvalidatedException(reason, getNodeEngine().getLocalMember().getAddress());
        }
        return error;
    }

    private Throwable toException(Address initiator, JetPacket packet) {
        Object object = getNodeEngine().getSerializationService().toObject(packet.toByteArray());

        if (object instanceof Throwable) {
            return new CombinedJetException(Arrays.asList((Throwable) object, new ApplicationInvalidatedException(initiator)));
        } else if (object instanceof JetPacket) {
            return new ApplicationInvalidatedException(initiator, (JetPacket) object);
        } else {
            return new ApplicationInvalidatedException(object, initiator);
        }
    }

    public int notifyContainersExecution(JetPacket jetPacket) {
        AtomicInteger counter = this.registrationCounters.get(jetPacket.getContainerId());

        if (counter == null) {
            return JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE;
        }

        if (counter.decrementAndGet() <= 0) {
            AtomicInteger membersCounter = new AtomicInteger(getNodeEngine().getClusterService().getMembers().size() - 1);
            this.registrationCounters.put(jetPacket.getContainerId(), membersCounter);
            this.registrations.get(jetPacket.getContainerId()).offer(true);
        }

        return 0;
    }

    // Here we should interrupt current application's execution
    public void invalidateApplicationInCluster(Object reason) {
        for (MemberImpl member : getNodeEngine().getClusterService().getMemberImpls()) {
            if (!member.localMember()) {
                JetPacket jetPacket = new JetPacket(
                        getApplicationName().getBytes(),
                        toBytes(reason)
                );

                jetPacket.setHeader(JetPacket.HEADER_JET_INVALIDATE_APPLICATION);
                discoveryService.getSocketWriters().get(member.getAddress()).sendServicePacket(jetPacket);
            } else {
                invalidateApplicationLocal(reason);
            }
        }
    }

    @Override
    public void deployNetworkEngine() {
        this.discoveryService.executeDiscovery();
        this.startNetWorkEngine();
    }

    private byte[] toBytes(Object reason) {
        if (reason == null) {
            return null;
        }

        return getNodeEngine().getSerializationService().toBytes(reason);
    }

    @Override
    public void registerShufflingReceiver(int taskID,
                                          ContainerContext containerContext,
                                          Address address,
                                          ShufflingReceiver receiver) {
        DataContainer dataContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = dataContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingReceiver(address, receiver);
        discoveryService.getSocketReaders().get(address).registerConsumer(receiver.getRingBufferActor());
    }

    @Override
    public void registerShufflingSender(int taskID,
                                        ContainerContext containerContext,
                                        Address address,
                                        ShufflingSender sender) {
        DataContainer dataContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = dataContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingSender(address, sender);
        this.discoveryService.getSocketWriters().get(address).registerProducer(sender.getRingBufferActor());
    }

    @Override
    public BlockingQueue<Object> synchronizeWithOtherNodes(DataContainer container) {
        if (this.registrations.size() > 0) {
            BlockingQueue<Object> queue = this.registrations.get(container.getID());

            for (Address address : discoveryService.getSocketWriters().keySet()) {
                JetPacket jetPacket = new JetPacket(container.getID(), getApplicationName().getBytes());
                jetPacket.setHeader(JetPacket.HEADER_JET_CONTAINER_STARTED);
                this.discoveryService.getSocketWriters().get(address).sendServicePacket(jetPacket);
            }

            return queue;
        } else {
            BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
            queue.offer(true);
            return queue;
        }
    }

    @Override
    public Map<Integer, DataContainer> getContainersCache() {
        return this.containersCache;
    }

    @Override
    public void setDag(DAG dag) {
        this.dag = dag;
    }

    @Override
    public Config getConfig() {
        return getNodeEngine().getConfig();
    }

    @Override
    public DAG getDag() {
        return this.dag;
    }

    @Override
    public DataContainer getContainerByVertex(Vertex vertex) {
        return this.vertex2ContainerCache.get(vertex);
    }

    @Override
    public void registerContainer(Vertex vertex, DataContainer container) {
        this.containers.add(container);
        this.vertex2ContainerCache.put(vertex, container);
        this.containersCache.put(container.getID(), container);

        int remoteMembersCount = getNodeEngine().getClusterService().getMembers().size() - 1;

        if (remoteMembersCount > 0) {
            this.registrations.put(container.getID(), new LinkedBlockingQueue<Object>());
            this.registrationCounters.put(container.getID(), new AtomicInteger(remoteMembersCount));
        }
    }

    @Override
    protected void wakeUpExecutor() {
        getApplicationContext().getExecutorContext().getApplicationMasterStateMachineExecutor().wakeUp();
    }
}
