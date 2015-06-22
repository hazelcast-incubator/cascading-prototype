package com.hazelcast.yarn.impl.statemachine;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;
import java.util.concurrent.BlockingQueue;
import com.hazelcast.yarn.api.executor.Task;
import com.hazelcast.yarn.impl.SettableFuture;
import com.hazelcast.yarn.api.executor.Payload;
import java.util.concurrent.LinkedBlockingDeque;
import com.hazelcast.yarn.impl.container.RequestPayLoad;
import com.hazelcast.yarn.api.statemachine.StateMachine;
import com.hazelcast.yarn.api.executor.ApplicationExecutor;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.StateMachineEvent;
import com.hazelcast.yarn.api.statemachine.StateMachineState;
import com.hazelcast.yarn.api.statemachine.StateMachineOutput;
import com.hazelcast.yarn.api.statemachine.StateMachineRequest;
import com.hazelcast.yarn.api.statemachine.InvalidEventException;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;

public abstract class AbstractStateMachineImpl<Input extends StateMachineEvent, State extends StateMachineState, Output extends StateMachineOutput> implements StateMachine<Input, State, Output> {
    private final String name;
    private final ILogger logger;
    private final Map<State, Map<Input, State>> stateTransitionMatrix;

    private volatile Output output;
    private volatile State state = defaultState();

    private final ApplicationContext applicationContext;
    private final StateMachineRequestProcessor<Input> processor;
    private final BlockingQueue<RequestPayLoad<Input, Output>> eventsQueue = new LinkedBlockingDeque<RequestPayLoad<Input, Output>>();

    protected abstract ApplicationExecutor getExecutor();

    protected AbstractStateMachineImpl(String name,
                                       Map<State, Map<Input, State>> stateTransitionMatrix,
                                       StateMachineRequestProcessor<Input> processor,
                                       NodeEngine nodeEngine,
                                       ApplicationContext applicationContext) {
        this.name = name;
        this.processor = processor;
        this.applicationContext = applicationContext;
        this.stateTransitionMatrix = stateTransitionMatrix;
        this.logger = nodeEngine.getLogger(this.getClass());
        this.getExecutor().addDistributed(new EventsProcessor(this.eventsQueue));
    }

    protected abstract State defaultState();

    @Override
    public State currentState() {
        return state;
    }

    public <P> Future<Output> handleRequest(StateMachineRequest<Input, P> request) {
        RequestPayLoad<Input, Output> payLoad = new RequestPayLoad<Input, Output>(request.getContainerEvent(), request.getPayLoad());
        this.eventsQueue.offer(payLoad);
        return payLoad.getFuture();
    }

    protected abstract Output output(Input input, State nextState);

    @Override
    public Output getOutput() {
        return output;
    }

    private class EventsProcessor implements Task {
        private final Queue<RequestPayLoad<Input, Output>> requestsQueue;

        EventsProcessor(Queue<RequestPayLoad<Input, Output>> requestsQueue) {
            this.requestsQueue = requestsQueue;
        }

        @Override
        public boolean executeTask(Payload payload) {
            RequestPayLoad<Input, Output> requestHolder = this.requestsQueue.poll();

            if (requestHolder == null) {
                payload.set(false);
                return true;
            }

            Input event = requestHolder.getEvent();
            SettableFuture<Output> future = requestHolder.getFuture();

            try {
                Map<Input, State> transmissions = stateTransitionMatrix.get(state);

                if (transmissions == null) {
                    future.setException(new InvalidEventException(event, state, name));
                    return true;
                }

                State nextState = transmissions.get(event);

                if (nextState != null) {
                    if (processor != null) {
                        processor.processRequest(requestHolder.getEvent(), requestHolder.getPayLoad());
                    }

                    state = nextState;
                    output = output(event, nextState);
                    future.set(output);
                } else {
                    output = output(event, null);
                    future.setException(new InvalidEventException(event, state, name));
                }
            } catch (Throwable e) {
                logger.warning(e.getMessage(), e);
                future.setException(e);
            }

            return true;
        }
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }
}
