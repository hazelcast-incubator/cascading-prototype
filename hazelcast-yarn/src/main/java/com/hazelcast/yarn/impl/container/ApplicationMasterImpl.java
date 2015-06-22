package com.hazelcast.yarn.impl.container;

import java.util.Map;
import java.util.List;
import java.util.Queue;
import java.util.Arrays;
import java.util.Collections;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.instance.MemberImpl;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.yarn.api.CombinedYarnException;

import com.hazelcast.yarn.impl.hazelcast.YarnPacket;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.container.ExecutionErrorHolder;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ContainerStateMachine;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingSender;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.impl.actor.shuffling.io.ShufflingReceiver;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.application.ApplicationInvalidatedException;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.ApplicationMasterStateMachineImpl;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.ExecutionCompletedRequest;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.ExecutionInterruptedRequest;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.InvalidateApplicationRequest;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.processors.ApplicationMasterPayLoadFactory;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterStateMachineFactory;

public class ApplicationMasterImpl extends
        AbstractServiceContainer<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
        implements ApplicationMaster {
    private static final InterruptedException APPLICATION_INTERRUPTED_EXCEPTION =
            new InterruptedException("Application has been interrupted");

    private final ILogger logger;

    private final AtomicBoolean invalidated = new AtomicBoolean(false);

    private final List<TupleContainer> containers = new CopyOnWriteArrayList<TupleContainer>();
    private final Map<Integer, TupleContainer> containersCache = new ConcurrentHashMap<Integer, TupleContainer>();

    private final Map<Vertex, TupleContainer> vertex2ContainerCache = new ConcurrentHashMap<Vertex, TupleContainer>();

    private final AtomicInteger executedContainers = new AtomicInteger(0);
    private final AtomicInteger interruptedContainers = new AtomicInteger(0);

    private final AtomicReference<BlockingQueue<Object>> executionMailBox = new AtomicReference<BlockingQueue<Object>>(null);
    private final AtomicReference<BlockingQueue<Object>> interruptionFutureHolder = new AtomicReference<BlockingQueue<Object>>(null);

    private static final ApplicationMasterStateMachineFactory STATE_MACHINE_FACTORY = new ApplicationMasterStateMachineFactory() {
        @Override
        public ContainerStateMachine<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> newStateMachine(String name, StateMachineRequestProcessor<ApplicationMasterEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext) {
            return new ApplicationMasterStateMachineImpl(name, processor, nodeEngine, applicationContext);
        }
    };

    private volatile DAG dag;

    private final ConcurrentMap<Integer, BlockingQueue<Object>> registrations = new ConcurrentHashMap<Integer, BlockingQueue<Object>>();
    private final ConcurrentMap<Integer, AtomicInteger> registrationCounters = new ConcurrentHashMap<Integer, AtomicInteger>();

    public ApplicationMasterImpl(
            ApplicationContext applicationContext
    ) {
        super(STATE_MACHINE_FACTORY, applicationContext.getNodeEngine(), applicationContext);
        this.logger = applicationContext.getNodeEngine().getLogger(ApplicationMaster.class);
    }

    @Override
    public void processRequest(ApplicationMasterEvent event, Object payLoad) throws Exception {
        ContainerPayLoadProcessor processor = ApplicationMasterPayLoadFactory.getProcessor(event, this);

        if (processor != null) {
            processor.process(payLoad);
        }
    }

    @Override
    public void handleContainerInterrupted(TupleContainer tapContainer) {
        if (interruptedContainers.incrementAndGet() >= containers.size()) {
            this.handleContainerRequest(new ExecutionInterruptedRequest());

            this.interruptedContainers.set(0);
            this.executedContainers.set(this.containers.size());
            this.interruptionFutureHolder.get().offer(ApplicationMasterResponse.SUCCESS);

            this.addToExecutionMailBox(APPLICATION_INTERRUPTED_EXCEPTION);
        }
    }

    @Override
    public void handleContainerCompleted(TupleContainer container) {
        if (this.executedContainers.decrementAndGet() <= 0) {
            this.executedContainers.set(containers.size());
            this.handleContainerRequest(new ExecutionCompletedRequest());
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
        this.addToExecutionMailBox(executionError.getError());
    }

    @Override
    public List<TupleContainer> containers() {
        return Collections.unmodifiableList(this.containers);
    }

    @Override
    public int receiveYarnPacket(YarnPacket yarnPacket) {
        int header = yarnPacket.getHeader();

        switch (header) {
            /*Request - bytes for tuple chunk*/
            /*Request shuffling channel closed*/
            case YarnPacket.HEADER_YARN_TUPLE_CHUNK:
            case YarnPacket.HEADER_YARN_SHUFFLER_CLOSED:
            case YarnPacket.HEADER_YARN_SHUFFLER_FINALIZING:
                return this.resolvePacket(yarnPacket, true);

            case YarnPacket.HEADER_YARN_INVALIDATE_APPLICATION:
                executeApplicationInvalidation(yarnPacket);
                return 1;

            case YarnPacket.HEADER_YARN_TUPLE_NO_APP_FAILURE:
            case YarnPacket.HEADER_YARN_TUPLE_NO_TASK_FAILURE:
            case YarnPacket.HEADER_YARN_TUPLE_NO_MEMBER_FAILURE:
            case YarnPacket.HEADER_YARN_CHUNK_WRONG_CHUNK_FAILURE:
            case YarnPacket.HEADER_YARN_UNKNOWN_EXCEPTION_FAILURE:
            case YarnPacket.HEADER_YARN_TUPLE_NO_CONTAINER_FAILURE:
            case YarnPacket.HEADER_YARN_APPLICATION_IS_NOT_EXECUTING:
                this.invalidateApplication(yarnPacket);
                return 0;

            case YarnPacket.HEADER_YARN_CONTAINER_STARTED:
                return this.notifyContainersExecution(yarnPacket);
        }

        return 1;
    }

    public void addToExecutionMailBox(Object object) {
        BlockingQueue<Object> executionMailBox = this.executionMailBox.get();

        if (executionMailBox != null) {
            executionMailBox.offer(object);
            this.executionMailBox.set(null);
        }
    }

    private void executeApplicationInvalidation(Object reason) {
        if (invalidated.compareAndSet(false, true)) {
            Throwable error;
            if (reason instanceof YarnPacket) {
                YarnPacket packet = (YarnPacket) reason;
                Address initiator = packet.getConn().getEndPoint();

                error = packet.toByteArray() == null ?
                        new ApplicationInvalidatedException(initiator) : toException(initiator, (YarnPacket) reason);
            } else if (reason instanceof Address) {
                error = new ApplicationInvalidatedException((Address) reason);
            } else if (reason instanceof Throwable) {
                error = new CombinedYarnException(
                        Arrays.asList(
                                (Throwable) reason,
                                new ApplicationInvalidatedException(getNodeEngine().getLocalMember().getAddress())
                        )
                );
            } else {
                error = new ApplicationInvalidatedException(reason, getNodeEngine().getLocalMember().getAddress());
            }

            this.handleContainerRequest(new InvalidateApplicationRequest());
            this.addToExecutionMailBox(error);

            for (Queue<Object> registrationFuture : this.registrations.values()) {
                registrationFuture.offer(error);
            }

            BlockingQueue<Object> mailBox = this.interruptionFutureHolder.get();

            if (mailBox != null) {
                mailBox.offer(error);
            }
        }
    }

    private Throwable toException(Address initiator, YarnPacket packet) {
        Object object = getNodeEngine().getSerializationService().toObject(packet.toByteArray());

        if (object instanceof Throwable) {
            return new CombinedYarnException(Arrays.asList((Throwable) object, new ApplicationInvalidatedException(initiator)));
        } else if (object instanceof YarnPacket) {
            return new ApplicationInvalidatedException(initiator, (YarnPacket) object);
        } else {
            return new ApplicationInvalidatedException(object, initiator);
        }
    }

    private int notifyContainersExecution(YarnPacket yarnPacket) {
        AtomicInteger counter = this.registrationCounters.get(yarnPacket.getContainerId());

        if (counter == null) {
            return YarnPacket.HEADER_YARN_TUPLE_NO_CONTAINER_FAILURE;
        }

        if (counter.decrementAndGet() <= 0) {
            AtomicInteger membersCounter = new AtomicInteger(this.getNodeEngine().getClusterService().getMembers().size() - 1);
            this.registrationCounters.put(yarnPacket.getContainerId(), membersCounter);
            this.registrations.get(yarnPacket.getContainerId()).offer(true);
        }

        return 0;
    }

    // Here we should interrupt current application's execution
    public void invalidateApplication(Object reason) {
        for (MemberImpl member : getNodeEngine().getClusterService().getMemberImpls()) {
            if (!member.localMember()) {
                YarnPacket yarnPacket = new YarnPacket(
                        getApplicationName().getBytes(),
                        toBytes(reason)
                );

                yarnPacket.setHeader(YarnPacket.HEADER_YARN_INVALIDATE_APPLICATION);
                ((NodeEngineImpl) this.getNodeEngine()).getNode().getConnectionManager().transmit(yarnPacket, member.getAddress());
            } else {
                executeApplicationInvalidation(reason);
            }
        }
    }

    private byte[] toBytes(Object reason) {
        if (reason == null) {
            return null;
        }

        return getNodeEngine().getSerializationService().toBytes(reason);
    }

    private int resolvePacket(YarnPacket yarnPacket, boolean isRequest) {
        TupleContainer tupleContainer = this.containersCache.get(yarnPacket.getContainerId());

        if (tupleContainer == null) {
            logger.warning("No such container with containerId=" +
                    yarnPacket.getContainerId() + " yarnPacket=" +
                    yarnPacket + ". Application will be interrupted.");
            return YarnPacket.HEADER_YARN_TUPLE_NO_CONTAINER_FAILURE;
        }

        ContainerTask containerTask = tupleContainer.getTasksCache().get(yarnPacket.getTaskID());

        if (containerTask == null) {
            logger.warning("No such task in container with containerId=" + yarnPacket.getContainerId() +
                    " taskId=" + yarnPacket.getTaskID() +
                    " yarnPacket=" + yarnPacket + ". Application will be interrupted.");
            return YarnPacket.HEADER_YARN_TUPLE_NO_TASK_FAILURE;
        }

        if (isRequest) {
            return this.notifyShufflingReceiver(yarnPacket, containerTask);
        }

        return -1;
    }

    private int notifyShufflingReceiver(YarnPacket yarnPacket, ContainerTask containerTask) {
        ShufflingReceiver receiver = containerTask.getShufflingReceiver(
                yarnPacket.getConn().getEndPoint()
        );

        if (receiver == null) {
            logger.warning("Packet " + yarnPacket + " received - but receiver hasn't been found");
            return YarnPacket.HEADER_YARN_TUPLE_NO_MEMBER_FAILURE;
        }

        if (yarnPacket.getHeader() == YarnPacket.HEADER_YARN_SHUFFLER_CLOSED) {
            receiver.close();
            return 0;
        } else if (yarnPacket.getHeader() == YarnPacket.HEADER_YARN_SHUFFLER_FINALIZING) {
            receiver.handleReceiverFinalizing();
            return 0;
        } else {
            try {
                if (receiver.consume(yarnPacket.toByteArray())) {
                    return 0;
                } else {
                    invalidateApplication(yarnPacket);
                    return 0;
                }
            } catch (Throwable e) {
                this.logger.warning(e.getMessage(), e);
                invalidateApplication(e);
                return 0;
            }
        }
    }

    @Override
    public void registerShufflingReceiver(int taskID, ContainerContext containerContext, Member member, ShufflingReceiver receiver) {
        TupleContainer tupleContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = tupleContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingReceiver(member, receiver);
    }

    @Override
    public void registerShufflingSender(int taskID, ContainerContext containerContext, Member member, ShufflingSender sender) {
        TupleContainer tupleContainer = this.containersCache.get(containerContext.getID());
        ContainerTask containerTask = tupleContainer.getTasksCache().get(taskID);
        containerTask.registerShufflingSender(member, sender);
    }

    @Override
    public BlockingQueue<Object> synchronizeWithOtherNodes(TupleContainer container) {
        if (this.registrations.size() > 0) {
            BlockingQueue<Object> queue = this.registrations.get(container.getID());

            for (Member member : getNodeEngine().getClusterService().getMembers()) {
                if (!member.localMember()) {
                    YarnPacket yarnPacket = new YarnPacket(container.getID(), getApplicationName().getBytes());
                    yarnPacket.setHeader(YarnPacket.HEADER_YARN_CONTAINER_STARTED);
                    ((NodeEngineImpl) this.getNodeEngine()).getNode().getConnectionManager().transmit(yarnPacket, member.getAddress());
                }
            }

            return queue;
        } else {
            BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
            queue.offer(true);
            return queue;
        }
    }

    @Override
    public void setDag(DAG dag) {
        this.dag = dag;
    }

    @Override
    public DAG getDag() {
        return this.dag;
    }

    @Override
    public TupleContainer getContainerByVertex(Vertex vertex) {
        return this.vertex2ContainerCache.get(vertex);
    }

    @Override
    public void registerContainer(Vertex vertex, TupleContainer container) {
        this.containers.add(container);
        this.vertex2ContainerCache.put(vertex, container);
        this.containersCache.put(container.getID(), container);

        int remoteMembersCount = this.getNodeEngine().getClusterService().getMembers().size() - 1;

        if (remoteMembersCount > 0) {
            this.registrations.put(container.getID(), new LinkedBlockingQueue<Object>());
            this.registrationCounters.put(container.getID(), new AtomicInteger(remoteMembersCount));
        }
    }

    @Override
    protected void wakeUpExecutor() {
        getApplicationContext().getApplicationMasterStateMachineExecutor().wakeUp();
    }
}
