/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.application;


import java.util.Map;
import java.util.Set;

import com.hazelcast.core.Member;
import com.hazelcast.spi.Operation;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.spi.dag.DAG;

import java.util.concurrent.Callable;

import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.ExecutorService;

import com.hazelcast.jet.spi.config.JetConfig;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.api.hazelcast.InvocationFactory;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.hazelcast.AbstractApplicationClusterService;
import com.hazelcast.jet.impl.operation.application.GetAccumulatorsOperation;
import com.hazelcast.jet.impl.operation.application.ApplicationEventOperation;
import com.hazelcast.jet.impl.operation.application.LocalizationChunkOperation;
import com.hazelcast.jet.impl.operation.application.InterruptExecutionOperation;
import com.hazelcast.jet.impl.operation.application.AcceptLocalizationOperation;
import com.hazelcast.jet.impl.operation.application.InitApplicationRequestOperation;
import com.hazelcast.jet.impl.operation.application.SubmitApplicationRequestOperation;
import com.hazelcast.jet.impl.operation.application.ExecutionApplicationRequestOperation;
import com.hazelcast.jet.impl.operation.application.FinalizationApplicationRequestOperation;


public class ServerApplicationClusterService extends AbstractApplicationClusterService<Operation> {
    private final NodeEngine nodeEngine;
    private final JetService jetService;

    public ServerApplicationClusterService(String name,
                                           ExecutorService executorService,
                                           NodeEngine nodeEngine,
                                           JetService jetService) {
        super(name, executorService);

        this.nodeEngine = nodeEngine;
        this.jetService = jetService;
    }

    public Operation createInitApplicationInvoker(JetApplicationConfig config) {
        return new InitApplicationRequestOperation(
                this.name,
                config
        );
    }

    @Override
    public Operation createFinalizationInvoker() {
        return new FinalizationApplicationRequestOperation(
                this.name
        );
    }

    @Override
    public Operation createInterruptInvoker() {
        return new InterruptExecutionOperation(
                this.name
        );
    }

    @Override
    public Operation createExecutionInvoker() {
        return new ExecutionApplicationRequestOperation(
                this.name
        );
    }

    @Override
    public Operation createAccumulatorsInvoker() {
        return new GetAccumulatorsOperation(
                this.name
        );
    }

    @Override
    public Operation createSubmitInvoker(DAG dag) {
        return new SubmitApplicationRequestOperation(
                this.name,
                dag
        );
    }

    @Override
    public Operation createLocalizationInvoker(Chunk chunk) {
        return new LocalizationChunkOperation(this.name, chunk);
    }

    @Override
    public Operation createAcceptedLocalizationInvoker() {
        return new AcceptLocalizationOperation(this.name);
    }

    public Operation createEventInvoker(ApplicationEvent applicationEvent) {
        return new ApplicationEventOperation(
                applicationEvent,
                this.name
        );
    }

    @Override
    public Set<Member> getMembers() {
        return this.nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected JetApplicationConfig getConfig() {
        JetApplicationConfig config =
                ((JetConfig) this.nodeEngine.getConfig()).getJetApplicationCofig(this.name);

        if (config == null) {
            return new JetApplicationConfig();
        }

        return config;
    }

    @Override
    public <T> Callable<T> createInvocation(Member member,
                                            InvocationFactory<Operation> factory) {
        return new ServerApplicationInvocation<T>(
                factory.payLoad(),
                member.getAddress(),
                this.jetService,
                this.nodeEngine
        );
    }

    @Override
    protected <T> T toObject(Data data) {
        return this.nodeEngine.toObject(data);
    }

    @SuppressWarnings("unchecked")
    public Map<CounterKey, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception {
        return (Map<CounterKey, Accumulator>) callable.call();
    }
}
