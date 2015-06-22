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

package com.hazelcast.jet.api.application;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.impl.application.LocalizationResource;

public interface ApplicationClusterService {
    void initApplication(JetApplicationConfig config,
                         ApplicationStateManager applicationStateManager
    );

    void submitDag(DAG dag, ApplicationStateManager applicationStateManager);

    void localizeApplication(Set<LocalizationResource> localizedResources,
                             ApplicationStateManager applicationStateManager);

    Future executeApplication(ApplicationStateManager applicationStateManager);

    Future interruptApplication(ApplicationStateManager applicationStateManager);

    Future finalizeApplication(ApplicationStateManager applicationStateManager);

    Map<String, Accumulator> getAccumulators();
}
