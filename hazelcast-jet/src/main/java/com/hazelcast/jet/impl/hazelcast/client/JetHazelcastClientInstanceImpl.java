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

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.jet.api.config.JetConfig;
import com.hazelcast.jet.api.application.Initable;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.jet.api.config.JetClientConfig;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.jet.api.statemachine.application.ApplicationState;

public class JetHazelcastClientInstanceImpl extends HazelcastClientInstanceImpl
        implements JetHazelcastInstance {
    public JetHazelcastClientInstanceImpl(JetClientConfig config,
                                          ClientConnectionManagerFactory clientConnectionManagerFactory,
                                          AddressProvider externalAddressProvider) {
        super(config, clientConnectionManagerFactory, externalAddressProvider);
        getProxyManager().register(JetService.SERVICE_NAME, new ClientProxyFactory() {
            @Override
            public ClientProxy create(String name) {
                return new JetApplicationProxy(JetService.SERVICE_NAME, name);
            }
        });
    }

    @Override
    public Application getJetApplication(String applicationName) {
        return getJetApplication(applicationName, null);
    }

    @Override
    public JetClientConfig getClientConfig() {
        return (JetClientConfig) super.getClientConfig();
    }

    @Override
    public Application getJetApplication(String applicationName, JetApplicationConfig jetApplicationConfig) {
        Application application = getDistributedObject(JetService.SERVICE_NAME, applicationName);

        if (application.getApplicationState() == ApplicationState.NEW) {
            ((Initable) application).init(jetApplicationConfig);
        }

        return application;
    }

    @Override
    public JetConfig getConfig() {
        return (JetConfig) super.getConfig();
    }
}
