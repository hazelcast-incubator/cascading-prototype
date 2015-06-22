package com.hazelcast.yarn.impl.application;

import java.net.URL;
import java.util.Set;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.Future;

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.yarn.api.dag.DAG;

import java.util.concurrent.Executors;

import com.hazelcast.instance.MemberImpl;

import java.util.concurrent.ExecutorService;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.yarn.impl.ChunkIterator;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.yarn.impl.YarnThreadFactory;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.yarn.api.application.Initable;
import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.yarn.api.CombinedYarnException;
import com.hazelcast.yarn.api.hazelcast.OperationFactory;
import com.hazelcast.yarn.api.application.ApplicationProxy;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;
import com.hazelcast.yarn.impl.operation.application.ApplicationEventOperation;
import com.hazelcast.yarn.impl.operation.application.LocalizationChunkOperation;
import com.hazelcast.yarn.impl.operation.application.InterruptExecutionOperation;
import com.hazelcast.yarn.impl.operation.application.AcceptLocalizationOperation;
import com.hazelcast.yarn.impl.operation.application.InitApplicationRequestOperation;
import com.hazelcast.yarn.impl.operation.application.SubmitApplicationRequestOperation;

import com.hazelcast.yarn.impl.operation.application.ExecutionApplicationRequestOperation;
import com.hazelcast.yarn.impl.operation.application.FinalizationApplicationRequestOperation;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ApplicationProxyImpl extends AbstractDistributedObject<YarnService> implements ApplicationProxy, Initable {
    private final String name;
    private final NodeEngine nodeEngine;
    private final YarnService yarnService;
    private final Runnable applicationExecutor;
    private final Runnable applicationFinalizer;
    private final ExecutorService executorService;
    private final Runnable applicationInterrupter;

    private final Set<LocalizationResource> localizedResources;

    public ApplicationProxyImpl(String name, YarnService yarnService, NodeEngine nodeEngine) {
        super(nodeEngine, yarnService);

        this.name = name;
        this.nodeEngine = nodeEngine;
        this.yarnService = yarnService;
        this.localizedResources = new HashSet<LocalizationResource>();
        String hzName = ((NodeEngineImpl) this.nodeEngine).getNode().hazelcastInstance.getName();

        int applicationThreadPoolSize = nodeEngine.getConfig().getYarnApplicationConfig(name).getApplicationInvocationThreadPoolSize();
        this.executorService = Executors.newFixedThreadPool(applicationThreadPoolSize, new YarnThreadFactory("invoker-application-thread-" + this.name, hzName));

        this.applicationExecutor = new Runnable() {
            @Override
            public void run() {
                publishEvent(ApplicationEvent.EXECUTION_START);

                try {
                    OperationFactory operationFactory = new OperationFactory() {
                        @Override
                        public Operation operation() {
                            return new ExecutionApplicationRequestOperation(
                                    ApplicationProxyImpl.this.name
                            );
                        }
                    };

                    invokeInCluster(
                            operationFactory
                    );

                    publishEvent(ApplicationEvent.EXECUTION_SUCCESS);
                } catch (Throwable e) {
                    try {
                        publishEvent(ApplicationEvent.EXECUTION_FAILURE);
                    } catch (Throwable ee) {
                        reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
                    }
                    reThrow(e);
                }
            }
        };

        this.applicationInterrupter = new Runnable() {
            @Override
            public void run() {
                publishEvent(ApplicationEvent.INTERRUPTION_START);

                try {
                    OperationFactory operationFactory = new OperationFactory() {
                        @Override
                        public Operation operation() {
                            return new InterruptExecutionOperation(
                                    ApplicationProxyImpl.this.name
                            );
                        }
                    };


                    invokeInCluster(
                            operationFactory
                    );

                    publishEvent(ApplicationEvent.INTERRUPTION_SUCCESS);
                } catch (Exception e) {
                    try {
                        publishEvent(ApplicationEvent.INTERRUPTION_FAILURE);
                    } catch (Throwable ee) {
                        reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
                    }

                    reThrow(e);
                }
            }
        };

            this.applicationFinalizer = new Runnable() {
            @Override
            public void run() {
                publishEvent(ApplicationEvent.FINALIZATION_START);

                try {
                    OperationFactory operationFactory = new OperationFactory() {
                        @Override
                        public Operation operation() {
                            return new FinalizationApplicationRequestOperation(
                                    ApplicationProxyImpl.this.name
                            );
                        }
                    };

                    invokeInCluster(
                            operationFactory
                    );
                } catch (Exception e) {
                    try {
                        publishEvent(ApplicationEvent.FINALIZATION_FAILURE);
                    } catch (Throwable ee) {
                        reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
                    }

                    reThrow(e);
                }
            }
        };
    }

    private void reThrow(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new RuntimeException(e);
        }
    }

    public void init() {
        try {
            publishEvent(ApplicationEvent.INIT_START);

            OperationFactory operationFactory = new OperationFactory() {
                @Override
                public Operation operation() {
                    return new InitApplicationRequestOperation(name);
                }
            };

            invokeInCluster(
                    operationFactory
            );

            publishEvent(ApplicationEvent.INIT_SUCCESS);
        } catch (Exception e) {
            try {
                publishEvent(ApplicationEvent.INIT_FAILURE);
            } catch (Throwable ee) {
                reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
            }

            reThrow(e);
        }
    }

    private void localizeApplication() {
        try {
            publishEvent(ApplicationEvent.LOCALIZATION_START);

            Iterator<Chunk> iterator = new ChunkIterator(
                    localizedResources,
                    nodeEngine.getConfig().getYarnApplicationConfig(name)
            );

            while (iterator.hasNext()) {
                final Chunk chunk = iterator.next();

                OperationFactory operationFactory = new OperationFactory() {
                    @Override
                    public Operation operation() {
                        return new LocalizationChunkOperation(
                                name,
                                chunk
                        );
                    }
                };

                invokeInCluster(
                        operationFactory
                );
            }

            OperationFactory operationFactory = new OperationFactory() {
                @Override
                public Operation operation() {
                    return new AcceptLocalizationOperation(name);
                }
            };

            invokeInCluster(
                    operationFactory
            );
            publishEvent(ApplicationEvent.LOCALIZATION_SUCCESS);
        } catch (Exception e) {
            try {
                publishEvent(ApplicationEvent.LOCALIZATION_FAILURE);
            } catch (Throwable ee) {
                reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
            }

            reThrow(e);
        }
    }

    @Override
    public void submit(DAG dag, Class... classes) throws IOException {
        if (classes != null) {
            this.addResource(classes);
        }

        this.localizeApplication();
        this.submit0(dag);
    }

    @Override
    public Future execute() {
        return executorService.submit(this.applicationExecutor);
    }

    @Override
    public Future interrupt() {
        return executorService.submit(this.applicationInterrupter);
    }

    @Override
    public Future finalizeApplication() {
        return executorService.submit(this.applicationFinalizer);
    }

    private void submit0(final DAG dag) {
        try {
            this.publishEvent(ApplicationEvent.SUBMIT_START);

            OperationFactory operationFactory = new OperationFactory() {
                @Override
                public Operation operation() {
                    return new SubmitApplicationRequestOperation(
                            name,
                            dag
                    );
                }
            };

            this.invokeInCluster(
                    operationFactory
            );

            this.publishEvent(ApplicationEvent.SUBMIT_SUCCESS);
        } catch (Exception e) {
            try {
                this.publishEvent(ApplicationEvent.SUBMIT_FAILURE);
            } catch (Throwable ee) {
                this.reThrow(new CombinedYarnException(Arrays.asList(e, ee)));
            }

            this.reThrow(e);
        }
    }

    private void publishEvent(final ApplicationEvent applicationEvent) {
        this.invokeInCluster(
                new OperationFactory() {
                    @Override
                    public Operation operation() {
                        return new ApplicationEventOperation(applicationEvent, name);
                    }
                }
        );
    }

    private void invokeInCluster(OperationFactory operationFactory) {
        ClusterService cs = this.nodeEngine.getClusterService();
        Collection<MemberImpl> members = cs.getMemberImpls();
        List<Future> futureList = new ArrayList<Future>(members.size());

        for (MemberImpl member : members) {
            futureList.add(this.executorService.submit(
                    new ApplicationInvocation(
                            operationFactory.operation(),
                            member.getAddress(),
                            this.yarnService,
                            this.nodeEngine
                    )));
        }

        List<Throwable> errors = new ArrayList<Throwable>(futureList.size());

        for (Future future : futureList) {
            try {
                future.get(this.nodeEngine.getConfig().getYarnApplicationConfig(this.name).getYarnSecondsToAwait(), TimeUnit.SECONDS);
            } catch (Throwable e) {
                errors.add(e);
            }
        }

        if (errors.size() > 1) {
            throw new CombinedYarnException(errors);
        } else if (errors.size() == 1) {
            reThrow(errors.get(0));
        }
    }

    @Override
    public void addResource(Class... classes) throws IOException {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            this.localizedResources.add(new LocalizationResource(clazz));
        }
    }

    @Override
    public void addResource(URL url) throws IOException {
        this.localizedResources.add(new LocalizationResource(url));
    }

    @Override
    public void addResource(InputStream inputStream, String name, LocalizationResourceType resourceType) throws IOException {
        this.localizedResources.add(new LocalizationResource(inputStream, name, resourceType));
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void clearResources() {
        this.localizedResources.clear();
    }

    @Override
    public String getServiceName() {
        return YarnService.SERVICE_NAME;
    }
}
