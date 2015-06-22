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

package com.hazelcast.yarn;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.util.Records;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;


public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
    public static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getSimpleName());

    public static final int TIMEOUT = 2000;

    private FileSystem hdfs;

    private NMClient nmClient;

    private final HazelcastYarnProperties properties;

    private final YarnConfiguration yarnConfiguration;

    private final Path hazelcastPath;

    /**
     * Resource manager.
     */
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;

    private Map<ContainerId, HazelcastContainer> containers =
            new ConcurrentHashMap<ContainerId, HazelcastContainer>();

    public ApplicationMaster(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalStateException("Invalid arguments number");
        }

        this.yarnConfiguration = new YarnConfiguration();
        this.hdfs = FileSystem.get(this.yarnConfiguration);
        this.hazelcastPath = new Path(args[0]);

        String yarnConfigPath = args.length > 1 ? args[1] : "";
        this.properties = new HazelcastYarnProperties(yarnConfigPath);

        LOG.log(Level.INFO, "HazelcastUri: {0}. YarnConfig: {1}.",
                new Object[]{this.hazelcastPath.toUri(), yarnConfigPath}
        );
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            this.containers.remove(status.getContainerId());

            LOG.log(Level.INFO, "Container completed. Container id: {0}. State: {1}.",
                    new Object[]{status.getContainerId(), status.getState()});
        }
    }

    private boolean checkContainer(Container cont) {
        if (this.properties.clusterSize() <= this.containers.size()) {
            LOG.log(Level.INFO,
                    "Failed this.properties.clusterSize()=" + this.properties.clusterSize() +
                            " this.containers.size()=" + this.containers.size()
            );

            return false;
        }

        if (cont.getResource().getVirtualCores() < this.properties.cpuPerNode()
                || cont.getResource().getMemory() < this.properties.memoryPerNode()) {
            LOG.log(Level.INFO, "Container resources not sufficient requirements. Host: {0}, cpu: {1}, mem: {2}",
                    new Object[]{cont.getNodeId().getHost(), cont.getResource().getVirtualCores(),
                            cont.getResource().getMemory()});

            return false;
        }

        return true;
    }

    @Override
    public synchronized void onContainersAllocated(List<Container> containerList) {
        for (Container container : containerList) {
            if (checkContainer(container)) {
                try {
                    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                    Map<String, String> env = new HashMap<String, String>(System.getenv());
                    ctx.setEnvironment(env);

                    Map<String, LocalResource> resources = new HashMap<String, LocalResource>();

                    resources.put("hazelcast",
                            YarnUtil.createFileResource(
                                    this.hazelcastPath,
                                    this.hdfs,
                                    LocalResourceType.ARCHIVE
                            )
                    );

                    if (this.properties.customLibs() != null) {
                        resources.put("libs", YarnUtil.createFileResource(
                                        new Path(this.properties.customLibs()),
                                        this.hdfs,
                                        LocalResourceType.FILE
                                )
                        );
                    }

                    if (this.properties.jvmOpts() != null && !this.properties.jvmOpts().isEmpty()) {
                        env.put("JVM_OPTS", this.properties.jvmOpts());
                    }

                    ctx.setEnvironment(env);

                    String command = "cd ./hazelcast/*/bin/ && ./server.sh hazelcast.xml"
                            + " -J-Xmx" + container.getResource().getMemory() + "m"
                            + " -J-Xms" + container.getResource().getMemory() + "m"
                            + YarnUtil.LOGS;

                    LOG.log(Level.INFO, command);

                    ctx.setLocalResources(resources);

                    ctx.setCommands(
                            Collections.singletonList(command)
                    );

                    LOG.log(Level.INFO, "Launching container: {0}.", container.getId());

                    this.nmClient.startContainer(container, ctx);

                    this.containers.put(
                            container.getId(),
                            new HazelcastContainer(
                                    container.getId(),
                                    container.getNodeId(),
                                    container.getResource().getVirtualCores(),
                                    container.getResource().getMemory()
                            )
                    );
                } catch (Exception ex) {
                    LOG.log(Level.WARNING, "Error launching container " + container.getId(), ex);
                }
            } else {
                LOG.log(Level.INFO, "Checking failed for container=" + container.toString());
            }
        }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updated) {
        for (NodeReport node : updated) {
            if (node.getNodeState().isUnusable()) {
                for (HazelcastContainer container : this.containers.values()) {
                    if (container.getNodeId().equals(node.getNodeId())) {
                        this.containers.remove(container.getId());
                        LOG.log(Level.WARNING, "Node is unusable. Node: {0}, state: {1}.",
                                new Object[]{node.getNodeId().getHost(), node.getNodeState()});
                    }
                }

                LOG.log(Level.WARNING, "Node is unusable. Node: {0}, state: {1}.",
                        new Object[]{node.getNodeId().getHost(), node.getNodeState()});
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster master = new ApplicationMaster(args);
        master.start();
    }

    public void start() throws IOException, YarnException, InterruptedException {
        startNetworkManager();

        startResourceManager();
        runApplicationMaster();
    }

    private void startNetworkManager() {
        this.nmClient = NMClient.createNMClient();
        this.nmClient.init(this.yarnConfiguration);
        this.nmClient.start();
    }

    private void startResourceManager() {
        this.rmClient = AMRMClientAsync.createAMRMClientAsync(500, this);
        this.rmClient.init(this.yarnConfiguration);
        this.rmClient.start();
    }

    private boolean checkResources() {
        Resource availableRes = this.rmClient.getAvailableResources();

        return availableRes == null || availableRes.getMemory() >= this.properties.memoryPerNode()
                && availableRes.getVirtualCores() >= this.properties.cpuPerNode();
    }

    private void runApplicationMaster() throws IOException, YarnException, InterruptedException {
        this.rmClient.registerApplicationMaster("", 0, "");

        LOG.log(Level.INFO, "Application master running...");
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        try {
            while (!this.nmClient.isInState(Service.STATE.STOPPED)) {
                int running = this.containers.size();

                if (running < this.properties.clusterSize() && checkResources()) {
                    // Resource requirements for worker containers.
                    Resource capability = Records.newRecord(Resource.class);
                    capability.setMemory(this.properties.memoryPerNode());
                    capability.setVirtualCores(this.properties.cpuPerNode());

                    for (int i = 0; i < this.properties.clusterSize() - running; i++) {
                        // Make container requests to ResourceManager
                        AMRMClient.ContainerRequest containerAsk =
                                new AMRMClient.ContainerRequest(capability, null, null, priority);

                        this.rmClient.addContainerRequest(containerAsk);
                        LOG.log(Level.INFO, "Making request. Memory: {0}, cpu {1}.",
                                new Object[]{this.properties.memoryPerNode(), this.properties.cpuPerNode()});
                    }
                }

                TimeUnit.MILLISECONDS.sleep(TIMEOUT);
            }
        } catch (InterruptedException e) {
            this.rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "", "");
            LOG.log(Level.WARNING, "Application master has been interrupted.");
        } catch (Exception e) {
            this.rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "", "");
            LOG.log(Level.SEVERE, "Application master caused error.", e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        this.nmClient.stop();
    }

    @Override
    public float getProgress() {
        return 42;
    }

    @Override
    public void onShutdownRequest() {

    }
}
