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

import com.hazelcast.jet.api.config.JetConfig;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

public class JetHazelcastInstanceProxy extends HazelcastInstanceProxy implements JetHazelcastInstance {
    protected JetHazelcastInstanceProxy(HazelcastInstanceImpl original) {
        super(original);
    }

    public JetHazelcastIntanceImpl getOriginal() {
        final JetHazelcastIntanceImpl hazelcastInstance = (JetHazelcastIntanceImpl) original;

        if (hazelcastInstance == null) {
            throw new HazelcastInstanceNotActiveException();
        }

        return hazelcastInstance;
    }

    @Override
    public Application getJetApplication(String applicationName) {
        return getOriginal().getJetApplication(applicationName);
    }

    @Override
    public Application getJetApplication(String applicationName, JetApplicationConfig jetApplicationConfig) {
        return getOriginal().getJetApplication(applicationName, jetApplicationConfig);
    }

    public JetConfig getConfig() {
        return (JetConfig) super.getConfig();
    }
}
