package com.hazelcast.yarn.impl.application.localization.classloader;

import java.net.URL;
import java.io.InputStream;

public interface ProxyClassLoader {
    /**
     * Loads the class
     *
     * @param className
     * @param resolveIt
     * @return class
     */
    Class loadClass(String className, boolean resolveIt);

    /**
     * Loads the resource
     *
     * @param name
     * @return InputStream
     */
    InputStream loadResource(String name);

    /**
     * Finds the resource
     *
     * @param name
     * @return InputStream
     */
    URL findResource(String name);
}
