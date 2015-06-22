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

package com.hazelcast.jet.api.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.api.config.JetApplicationConfig;

public interface JetHazelcastInstance extends HazelcastInstance {
    /***
     * Returns an application with the specified name.
     *
     * @param applicationName - name of the application
     * @return distributed Jet application
     */
    Application getJetApplication(String applicationName);

    /***
     * Returns an application with the specified name.
     *
     * @param applicationName      - name of the application
     * @param jetApplicationConfig - config of the Jet application
     * @return distributed Jet application
     */
    Application getJetApplication(String applicationName, JetApplicationConfig jetApplicationConfig);
}
