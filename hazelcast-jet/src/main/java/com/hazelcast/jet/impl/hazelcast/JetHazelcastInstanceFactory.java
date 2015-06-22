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
import com.hazelcast.instance.NodeContext;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.DefaultHazelcastInstanceFactory;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

public class JetHazelcastInstanceFactory extends DefaultHazelcastInstanceFactory {
    public JetHazelcastIntanceImpl newHazelcastInstance(Config config, String instanceName,
                                                         NodeContext nodeContext) {
        try {
            return new JetHazelcastIntanceImpl(getInstanceName(instanceName, config), config, nodeContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected HazelcastInstanceProxy newHazelcastProxy(HazelcastInstanceImpl hazelcastInstance) {
        return new JetHazelcastInstanceProxy(hazelcastInstance);
    }

    public JetHazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        ServiceConfig myServiceConfig = new JetServiceConfig();
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(myServiceConfig);

        return (JetHazelcastInstance) super.newHazelcastInstance(config);
    }
}
