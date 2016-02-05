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

package com.hazelcast.jet.impl.operation.application;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.InterruptApplicationRequest;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class InterruptExecutionOperation extends AbstractJetApplicationRequestOperation {
    public InterruptExecutionOperation() {
    }

    public InterruptExecutionOperation(String name) {
        this(name, null);
    }

    public InterruptExecutionOperation(String name, NodeEngineImpl nodeEngine) {
        super(name);
        setNodeEngine(nodeEngine);
        setServiceName(JetService.SERVICE_NAME);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();
        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(
                new InterruptApplicationRequest()
        );

        JetApplicationConfig config = applicationContext.getJetApplicationConfig();
        long secondsToAwait = config.getJetSecondsToAwait();

        try {
            ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable interrupt application's execution");
            }
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }

        BlockingQueue<Object> mailBox = applicationMaster.getInterruptionMailBox();

        if (mailBox != null) {
            Object result = mailBox.poll(secondsToAwait, TimeUnit.SECONDS);
            if ((result != null) && (result instanceof Throwable)) {
                throw JetUtil.reThrow((Throwable) result);
            }
        }
    }
}
