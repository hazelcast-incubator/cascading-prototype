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

package com.hazelcast.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.io.FileInputStream;

public class HazelcastYarnProperties {
    public static final String JVM_OPTS = "JVM_OPTS";
    public static final String CLUSTER_SIZE = "CLUSTER_SIZE";
    public static final String CPU_PER_NODE = "CPU_PER_NODE";
    public static final String MEMORY_PER_NODE = "MEMORY_PER_NODE";
    public static final String HAZELCAST_WORK_DIR = "HAZELCAST_WORK_DIR";
    public static final String HAZELCAST_CUSTOM_LIBS = "HAZELCAST_CUSTOM_LIBS";

    private final String jvmOpts;
    private final int cpuPerNode;
    private final int clusterSize;
    private final int memoryPerNode;
    private final String customLibs;
    private final String hazelcastWorkDir;

    private static final String DEFAULT_JVM_OPTS = "";
    private static final String DEFAULT_CPU_PER_NODE = "1";
    private static final String DEFAULT_CLUSTER_SIZE = "2";
    private static final String DEFAULT_MEMORY_PER_NODE = "1024";
    private static final String DEFAULT_HAZELCAST_WORK_DIR = "/hazelcast/workdir";

    public HazelcastYarnProperties(String propertyFile) {
        Properties properties = loadProperties(propertyFile);

        this.jvmOpts = properties.getProperty(JVM_OPTS, DEFAULT_JVM_OPTS);
        this.cpuPerNode = Integer.valueOf(properties.getProperty(CPU_PER_NODE, DEFAULT_CPU_PER_NODE));
        this.clusterSize = Integer.valueOf(properties.getProperty(CLUSTER_SIZE, DEFAULT_CLUSTER_SIZE));
        this.memoryPerNode = Integer.valueOf(properties.getProperty(MEMORY_PER_NODE, DEFAULT_MEMORY_PER_NODE));
        this.hazelcastWorkDir = properties.getProperty(HAZELCAST_WORK_DIR, DEFAULT_HAZELCAST_WORK_DIR);
        this.customLibs = properties.getProperty(HAZELCAST_CUSTOM_LIBS);
    }

    private Properties loadProperties(String propertyFile) {
        Properties properties = new Properties();

        try {
            File f = new File(propertyFile);
            if ((f.exists())
                    && (f.isFile())
                    && (f.canRead())) {
                properties.load(new FileInputStream(f));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return properties;
    }

    public int clusterSize() {
        return this.clusterSize;
    }

    public int memoryPerNode() {
        return this.memoryPerNode;
    }

    public int cpuPerNode() {
        return this.cpuPerNode;
    }

    public String jvmOpts() {
        return this.jvmOpts;
    }

    public String hazelcastWorkDir() {
        return this.hazelcastWorkDir;
    }

    public String customLibs() {
        return this.customLibs;
    }
}
