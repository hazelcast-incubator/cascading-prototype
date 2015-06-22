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

package org;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

public class HzNodeStart {
    public static JetHazelcastInstance instance;

    public static void main(String[] args) {
        String nodes = args[0];

        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        List<String> list = new ArrayList<String>();

        config.setProperty("hazelcast.operation.call.timeout.millis", "100000000");
        config.setProperty("hazelcast.io.thread.count", "3");

        Collections.addAll(list, nodes.split(";"));

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        instance = JetHazelcast.newHazelcastInstance(config);

        JetApplicationConfig jetApplicationConfig = new JetApplicationConfig("testApplication");
        instance.getConfig().addCustomServiceConfig(jetApplicationConfig);

        jetApplicationConfig.setChunkSize(4096);
        jetApplicationConfig.setMaxProcessingThreads(8);
        jetApplicationConfig.setContainerQueueSize(65536);
        jetApplicationConfig.setJetSecondsToAwait(100000);
        jetApplicationConfig.setDefaultTCPBufferSize(65536);
        jetApplicationConfig.setShufflingBatchSizeBytes(65536);
        jetApplicationConfig.setApplicationSecondsToAwait(100000);
        jetApplicationConfig.setIoThreadCount(Runtime.getRuntime().availableProcessors() / 4);
    }
}