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

package com.hazelcast.jet.impl.hazelcast.client;

import com.hazelcast.jet.impl.JetUtil;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

public class JetHazelcastClientProxy extends HazelcastClientProxy implements JetHazelcastInstance {
    public JetHazelcastClientProxy(JetHazelcastClientInstanceImpl client) {
        super(client);
    }

    protected JetHazelcastClientInstanceImpl getClient() {
        final JetHazelcastClientInstanceImpl c = (JetHazelcastClientInstanceImpl) client;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }
        return c;
    }

    private void checkApplicationName(String applicationName) {
        JetUtil.checkApplicationName(applicationName);
    }

    @Override
    public Application getJetApplication(String applicationName) {
        checkApplicationName(applicationName);
        return getJetApplication(applicationName, null);
    }

    @Override
    public Application getJetApplication(String applicationName, JetApplicationConfig jetApplicationConfig) {
        checkApplicationName(applicationName);
        return getClient().getJetApplication(applicationName, jetApplicationConfig);
    }
}
