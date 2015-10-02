package org;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.yarn.impl.hazelcast.YarnHazelcast;

public class HzNodeStart {
    public static YarnHazelcastInstance instance;

    public static void main(String[] args) {
        String nodes = args[0];

        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        List<String> list = new ArrayList<String>();

        config.setProperty("hazelcast.operation.call.timeout.millis", "100000000");
        config.setProperty("hazelcast.io.thread.count", "30");

        Collections.addAll(list, nodes.split(";"));

        networkConfig.getJoin().getTcpIpConfig().setMembers(
                list
        );

        instance = YarnHazelcast.newHazelcastInstance(config);

        instance.getConfig().getYarnApplicationConfig("testApplication").setShufflingBatchSizeBytes(1024 * 1024 * 100);
        instance.getConfig().getYarnApplicationConfig("testApplication").setTupleChunkSize(65536);
        instance.getConfig().getYarnApplicationConfig("testApplication").setContainerQueueSize(65536);
        instance.getConfig().getYarnApplicationConfig("testApplication").setYarnSecondsToAwait(100000);
        instance.getConfig().getYarnApplicationConfig("testApplication").setApplicationSecondsToAwait(100000);
        instance.getConfig().getYarnApplicationConfig("testApplication").setMaxProcessingThreads(Runtime.getRuntime().availableProcessors());
    }
}