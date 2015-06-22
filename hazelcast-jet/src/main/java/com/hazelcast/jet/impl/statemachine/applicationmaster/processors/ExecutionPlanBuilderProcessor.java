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

package com.hazelcast.jet.impl.statemachine.applicationmaster.processors;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

import com.hazelcast.core.IFunction;
import com.hazelcast.spi.NodeEngine;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Edge;
import com.hazelcast.jet.api.dag.Vertex;

import java.lang.reflect.InvocationTargetException;

import com.hazelcast.jet.api.container.DataChannel;
import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.data.tuple.TupleFactory;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.impl.container.DefaultDataChannel;
import com.hazelcast.jet.api.processor.ProcessorDescriptor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.data.tuple.DefaultTupleFactory;
import com.hazelcast.jet.api.processor.ContainerProcessorFactory;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.impl.container.DefaultProcessingContainer;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.statemachine.container.requests.StartTupleContainerRequest;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ExecutionPlanBuilderProcessor implements ContainerPayLoadProcessor<DAG> {
    private final NodeEngine nodeEngine;
    private final TupleFactory tupleFactory;
    private final ClassLoader applicationClassLoader;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;


    private final IFunction<Vertex, DataContainer> containerProcessingCreator = new IFunction<Vertex, DataContainer>() {
        @Override
        public DataContainer apply(Vertex vertex) {
            ProcessorDescriptor descriptor = vertex.getDescriptor();
            String className = descriptor.getContainerProcessorFactoryClazz();
            Object[] args = descriptor.getFactoryArgs();

            ContainerProcessorFactory processorFactory = containerProcessorFactory(className, args);

            DataContainer container = new DefaultProcessingContainer(
                    vertex,
                    processorFactory,
                    nodeEngine,
                    applicationContext,
                    tupleFactory
            );

            applicationMaster.registerContainer(vertex, container);

            return container;
        }
    };

    public ExecutionPlanBuilderProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.nodeEngine = applicationMaster.getNodeEngine();
        this.applicationContext = applicationMaster.getApplicationContext();
        this.applicationClassLoader = this.applicationContext.getLocalizationStorage().getClassLoader();
        this.tupleFactory = new DefaultTupleFactory();
    }

    @Override
    public void process(DAG dag) throws Exception {
        checkNotNull(dag);

        //Process dag and container's chain building
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();

        Map<Vertex, DataContainer> vertex2ContainerMap = new HashMap<Vertex, DataContainer>(dag.getVertices().size());

        System.out.println("process1");

        List<DataContainer> roots = new ArrayList<DataContainer>();

        System.out.println("process2");

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();

            System.out.println("vertex=" + vertex.getName());

            List<Edge> edges = vertex.getInputEdges();
            DataContainer next = containerProcessingCreator.apply(vertex);

            System.out.println("vertex=" + vertex.getName() + " out");

            vertex2ContainerMap.put(vertex, next);

            if (edges.size() == 0) {
                roots.add(next);
            } else {
                for (Edge edge : edges) {
                    join(vertex2ContainerMap.get(edge.getInputVertex()), edge, next);
                }
            }
        }
        System.out.println("process3");

        JetApplicationConfig jetApplicationConfig = this.applicationContext.getJetApplicationConfig();

        long secondsToAwait =
                jetApplicationConfig.getApplicationSecondsToAwait();

        System.out.println("process4");

        for (DataContainer container : roots) {
            applicationMaster.addFollower(container);
        }

        System.out.println("process5");

        this.applicationContext.getExecutorContext().getProcessingExecutor().startWorkers();

        System.out.println("process6");

        this.applicationContext.getApplicationMaster().deployNetworkEngine();

        System.out.println("process7");

        for (DataContainer container : this.applicationMaster.containers()) {
            container.handleContainerRequest(new StartTupleContainerRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }

        System.out.println("process8");
    }

    private DataContainer join(DataContainer container, Edge edge, DataContainer nextContainer) {
        linkWithChannel(container, nextContainer, edge);
        return nextContainer;
    }

    private void linkWithChannel(DataContainer container,
                                 DataContainer next,
                                 Edge edge
    ) {
        if ((container != null) && (next != null)) {
            DataChannel channel = new DefaultDataChannel(container, next, edge);

            container.addFollower(next);
            next.addPredecessor(container);

            container.addOutputChannel(channel);
            next.addInputChannel(channel);
        }
    }

    @SuppressWarnings("unchecked")
    private ContainerProcessorFactory containerProcessorFactory(String className, Object... args) {
        try {
            Class<ContainerProcessorFactory> clazz =
                    (Class<ContainerProcessorFactory>) Class.forName(
                            className,
                            true,
                            this.applicationClassLoader
                    );

            int i = 0;
            Class[] argsClasses = new Class[args.length];

            for (Object obj : args) {
                argsClasses[i++] = obj.getClass();
            }

            Constructor<ContainerProcessorFactory> resultConstructor = null;

            for (Constructor constructor : clazz.getConstructors()) {
                if (constructor.getParameterTypes().length == argsClasses.length) {
                    boolean valid = true;
                    for (int idx = 0; idx < argsClasses.length; idx++) {
                        if (!constructor.getParameterTypes()[idx].isAssignableFrom(argsClasses[idx])) {
                            valid = false;
                            break;
                        }
                    }

                    if (valid) {
                        resultConstructor = constructor;
                    }
                }
            }

            if (resultConstructor == null) {
                throw new IllegalStateException("No constructor with arguments"
                        + Arrays.toString(argsClasses)
                        + " className=" + className
                );
            }

            return resultConstructor.newInstance(args);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
