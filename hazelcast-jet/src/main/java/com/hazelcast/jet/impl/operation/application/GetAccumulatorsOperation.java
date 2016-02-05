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

import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.api.application.ApplicationContext;

public class GetAccumulatorsOperation extends AbstractJetApplicationRequestOperation {
    public GetAccumulatorsOperation(String name) {
        super(name);
    }

    public GetAccumulatorsOperation(String name, NodeEngineImpl nodeEngine) {
        super(name);
        setNodeEngine(nodeEngine);
        setServiceName(JetService.SERVICE_NAME);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        this.result = applicationContext.getAccumulators();
    }
}
