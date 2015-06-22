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


import com.hazelcast.jet.api.Dummy;

import java.util.concurrent.TimeUnit;

import com.hazelcast.jet.api.container.DataContainer;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.statemachine.container.requests.InvalidateContainersRequest;

public class InterruptionFailureProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final long secondsToAwait;
    private final ApplicationMaster applicationMaster;

    public InterruptionFailureProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        ApplicationContext applicationContext = applicationMaster.getApplicationContext();
        JetApplicationConfig config = applicationContext.getJetApplicationConfig();
        this.secondsToAwait = config.getApplicationSecondsToAwait();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        for (DataContainer container : applicationMaster.containers()) {
            container.handleContainerRequest(new InvalidateContainersRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }
    }
}
