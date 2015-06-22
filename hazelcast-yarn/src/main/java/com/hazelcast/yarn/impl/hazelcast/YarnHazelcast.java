package com.hazelcast.yarn.impl.hazelcast;

import java.util.Set;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.yarn.api.hazelcast.YarnHazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;

public class YarnHazelcast {
    private YarnHazelcast() {
    }

    /**
     * Shuts down all running Hazelcast Instances on this JVM.
     * It doesn't destroy all members of the
     * cluster but just the ones running on this JVM.
     *
     * @see #newHazelcastInstance(Config)
     */
    public static void shutdownAll() {
        HazelcastInstanceFactory.INSTANCE.shutdownAll();
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p>
     * To destroy all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HazelcastInstance (member)
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static YarnHazelcastInstance newHazelcastInstance(Config config) {
        return (YarnHazelcastInstance) HazelcastInstanceFactory.INSTANCE.newHazelcastInstance(config);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p>
     * To destroy all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     * <p>
     * Hazelcast will look into two places for the configuration file:
     * <ol>
     * <li>
     * System property: Hazelcast will first check if "hazelcast.config" system property is set to a file path.
     * Example: -Dhazelcast.config=C:/myhazelcast.xml.
     * </li>
     * <li>
     * Classpath: If config file is not set as a system property, Hazelcast will check classpath for hazelcast.xml file.
     * </li>
     * </ol>
     * If Hazelcast doesn't find any config file, it will start with the default configuration (hazelcast-default.xml)
     * located in hazelcast.jar.
     *
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static YarnHazelcastInstance newHazelcastInstance() {
        return (YarnHazelcastInstance) HazelcastInstanceFactory.INSTANCE.newHazelcastInstance(null);
    }

    /**
     * Returns an existing HazelcastInstance with instanceName.
     * <p>
     * To destroy all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param instanceName Name of the HazelcastInstance (member)
     * @return an existing HazelcastInstance
     * @see #newHazelcastInstance(Config)
     * @see #shutdownAll()
     */
    public static YarnHazelcastInstance getHazelcastInstanceByName(String instanceName) {
        return (YarnHazelcastInstance) HazelcastInstanceFactory.INSTANCE.getHazelcastInstance(instanceName);
    }

    /**
     * Gets or creates the HazelcastInstance with a certain name.
     * <p>
     * If a Hazelcast instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * @param config the Config.
     * @return the HazelcastInstance
     * @throws NullPointerException     if config is null.
     * @throws IllegalArgumentException if the instance name of the config is null or empty.
     */
    public static YarnHazelcastInstance getOrCreateHazelcastInstance(Config config) {
        return (YarnHazelcastInstance) HazelcastInstanceFactory.INSTANCE.getOrCreateHazelcastInstance(config);
    }


    /**
     * Returns all active/running HazelcastInstances on this JVM.
     * <p>
     * To destroy all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @return all active/running HazelcastInstances on this JVM
     * @see #newHazelcastInstance(Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #shutdownAll()
     */
    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return HazelcastInstanceFactory.INSTANCE.getAllHazelcastInstances();
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hazelcast threads.
     * <p>
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <tt>OutOfMemoryError</tt> is caught by Hazelcast threads
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setServerHandler(outOfMemoryHandler);
    }
}
