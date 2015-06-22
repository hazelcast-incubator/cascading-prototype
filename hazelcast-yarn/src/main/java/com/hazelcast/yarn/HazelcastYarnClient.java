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

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

/**
 * Yarn client.
 */
public class HazelcastYarnClient {
    public static final Logger LOG = Logger.getLogger(HazelcastYarnClient.class.getSimpleName());
    private final static String appName = "hazelcast";
    private final URI hazelcastUri;

    private HazelcastYarnProperties properties;

    private final FileSystem fs;
    private final String pathToAppJar;
    private final String pathToYarnConfig;
    private final YarnConfiguration conf;

    public static void main(String[] args) throws Exception {
        HazelcastYarnClient yarnClient = new HazelcastYarnClient(args);
        yarnClient.init();
    }

    private void init() throws Exception {
        YarnClient yarnClient = startYarnClient(this.conf);

        ApplicationId appId = submitApplication(yarnClient);
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        appState = process(yarnClient, appId, appState);

        LOG.log(Level.INFO, "Application {0} is {1}.", new Object[]{appId, appState});
    }

    public HazelcastYarnClient(String[] args) throws Exception {
        System.out.println("Starting yarn client...");
        this.conf = new YarnConfiguration();
        this.fs = FileSystem.get(this.conf);
        this.pathToYarnConfig = getYarnConfig(args);
        this.properties = new HazelcastYarnProperties(this.pathToYarnConfig);
        this.pathToAppJar = getAppMasterJarPath(args);

        this.hazelcastUri = setUpHazelcast(args).toUri();
    }

    private Path setUpHazelcast(String[] args) throws Exception {
        String pathToHazelcastZip = getHazelcastZipPath(args);

        return YarnUtil.createHDFSFile(
                this.fs,
                pathToHazelcastZip,
                properties.hazelcastWorkDir() + File.separator + "hazelcast.zip"
        );
    }

    private ContainerLaunchContext getContainerLaunchContext() {
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        String command = Environment.JAVA_HOME.$() +
                "/bin/java"
                + " -Xmx512m  " +
                ApplicationMaster.class.getName()
                + " "
                + this.hazelcastUri
                + " "
                + this.pathToYarnConfig
                + YarnUtil.LOGS;

        System.out.println(command);
        amContainer.setCommands(Collections.singletonList(command));
        return amContainer;
    }

    private ApplicationId submitApplication(YarnClient yarnClient)
            throws Exception {
        YarnClientApplication app = yarnClient.createApplication();

        Path appJar = YarnUtil.createHDFSFile(
                this.fs,
                this.pathToAppJar,
                this.properties.hazelcastWorkDir() + File.separator + YarnUtil.JAR_NAME
        );

        ContainerLaunchContext amContainer = getContainerLaunchContext();

        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setEnv(appMasterEnv, this.conf);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = getCapability();
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();

        appContext.setApplicationName(appName);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        ApplicationId appId = appContext.getApplicationId();
        LocalResource appMasterJar = YarnUtil.createFileResource(appJar, this.fs, LocalResourceType.FILE);
        amContainer.setLocalResources(Collections.singletonMap(appJar.getName(), appMasterJar));
        yarnClient.submitApplication(appContext);
        LOG.log(Level.INFO, "Submitted application. Application id: {0}", appId);
        return appId;
    }

    private Resource getCapability() {
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(1);
        return capability;
    }

    private YarnClient startYarnClient(YarnConfiguration conf) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        return yarnClient;
    }

    private YarnApplicationState process(YarnClient yarnClient,
                                         ApplicationId appId,
                                         YarnApplicationState appState)
            throws InterruptedException, YarnException, IOException {
        ApplicationReport appReport;

        while (appState == YarnApplicationState.NEW ||
                appState == YarnApplicationState.NEW_SAVING ||
                appState == YarnApplicationState.SUBMITTED ||
                appState == YarnApplicationState.ACCEPTED) {
            TimeUnit.SECONDS.sleep(1L);
            appReport = yarnClient.getApplicationReport(appId);

            if ((appState != YarnApplicationState.ACCEPTED)
                    && (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED)
                    ) {
                LOG.log(Level.INFO, "Application {0} is ACCEPTED.", appId);
            }

            appState = appReport.getYarnApplicationState();
        }

        return appState;
    }

    private void setEnv(Map<String, String> envs, YarnConfiguration conf) {
        for (String property : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(envs, Environment.CLASSPATH.name(),
                    property.trim(), File.pathSeparator);
        }

        Apps.addToEnvironment(envs,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*",
                File.pathSeparator);
    }

    public String getAppMasterJarPath(String[] args) {
        checkArguments(args);
        return args[0];
    }

    public String getHazelcastZipPath(String[] args) {
        checkArguments(args);
        return args[1];
    }

    public String getYarnConfig(String[] args) {
        return args.length == 3 ? args[2] : "";
    }

    private void checkArguments(String[] args) {
        if (args.length < 2) {
            throw new IllegalStateException("Usage <command> <pathToAppMasterJar> <pathToHazelcastZip> [<pathToYarnConfig>] ");
        }
    }

}
