package com.hazelcast.yarn.api.application;

import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Future;

import com.hazelcast.yarn.api.dag.DAG;
import com.hazelcast.yarn.impl.application.LocalizationResourceType;

/***
 * Represents abstract application
 */
public interface Application {
    /***
     * Submit dag to the cluster
     *
     * @param dag     - Direct acyclic graph, which describes calculation flow
     * @param classes - Classes which will be used during calculation process
     * @throws IOException
     */
    void submit(DAG dag, Class... classes) throws IOException;


    /***
     * Add classes to the calculation's classLoader
     *
     * @param classes - classes, which will be used during calculation
     * @throws IOException
     */
    void addResource(Class... classes) throws IOException;

    /***
     * Add all bytecode for url to the calculation classLoader
     *
     * @param url - source url with classes
     * @throws IOException
     */
    void addResource(URL url) throws IOException;

    /***
     * Add all bytecode for url to the calculation classLoader
     *
     * @param inputStream              - source inputStream with bytecode
     * @param name                     - name of the source
     * @param localizationResourceType - type of data stored in inputStream (JAR,CLASS,DATA)
     * @throws IOException
     */
    void addResource(InputStream inputStream, String name, LocalizationResourceType localizationResourceType) throws IOException;

    /***
     * Clear all submited resources
     */
    void clearResources();

    /***
     * @return Returns name for the application
     */
    String getName();

    /***
     * Execute application
     *
     * @return Future which will return execution's result
     */
    Future execute();

    /***
     * Interrupts application
     *
     * @return Future which will return interruption's result
     */
    Future interrupt();

    /***
     * Finalizes application
     *
     * @return Future which will return finalization's result
     */
    Future finalizeApplication();
}