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

package com.hazelcast.jet.impl.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.impl.JetUtil;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.jet.api.application.Initable;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.jet.api.statemachine.application.ApplicationState;

public class JetHazelcastIntanceImpl extends HazelcastInstanceImpl implements JetHazelcastInstance {

    public JetHazelcastIntanceImpl(String name, Config config, NodeContext nodeContext) throws Exception {
        super(name, config, nodeContext);
    }

    private void checkApplicationName(String applicationName) {
        JetUtil.checkApplicationName(applicationName);
    }

    @Override
    protected Node createNode(Config config, NodeContext nodeContext) {
        return new JetNode(this, config, nodeContext);
    }

    @Override
    public Application getJetApplication(String applicationName) {
        return getJetApplication(applicationName, null);
    }

    @Override
    public Application getJetApplication(String applicationName, JetApplicationConfig jetApplicationConfig) {
        checkApplicationName(applicationName);
        Application application = getDistributedObject(JetService.SERVICE_NAME, applicationName);

        if (application.getApplicationState() == ApplicationState.NEW) {
            ((Initable) application).init(jetApplicationConfig);
        }

        return application;
    }
}
