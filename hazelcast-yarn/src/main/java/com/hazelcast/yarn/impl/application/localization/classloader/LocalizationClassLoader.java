package com.hazelcast.yarn.impl.application.localization.classloader;

import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.net.MalformedURLException;

import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.yarn.api.application.localization.classloader.YarnClassLoaderException;

public class LocalizationClassLoader extends ClassLoader implements ProxyClassLoader {
    private final Map<String, Class> classes = new HashMap<String, Class>();

    private final Map<String, ClassLoaderEntry> classEntries = new HashMap<String, ClassLoaderEntry>();
    private final Map<String, ClassLoaderEntry> dataEntries = new HashMap<String, ClassLoaderEntry>();
    private final Map<String, ClassLoaderEntry> jarEntries = new HashMap<String, ClassLoaderEntry>();

    public LocalizationClassLoader(Map<LocalizationResourceDescriptor, ResourceStream> resources) {
        for (Map.Entry<LocalizationResourceDescriptor, ResourceStream> entry : resources.entrySet()) {
            LocalizationResourceDescriptor descriptor = entry.getKey();
            switch (descriptor.getResourceType()) {
                case JAR:
                    loadJarStream(entry.getValue());
                    break;
                case CLASS:
                    loadClassStream(descriptor, entry.getValue());
                    break;
                case DATA:
                    loadDataStream(descriptor, entry.getValue());
                    break;
            }
        }
    }

    private void loadClassStream(LocalizationResourceDescriptor descriptor, ResourceStream resourceStream) {
        byte[] classBytes = YarnUtil.readFully(resourceStream.getInputStream());
        classEntries.put(descriptor.getName(), new ClassLoaderEntry(classBytes, resourceStream.getBaseUrl()));
    }

    private void loadDataStream(LocalizationResourceDescriptor descriptor, ResourceStream resourceStream) {
        byte[] bytes = YarnUtil.readFully(resourceStream.getInputStream());
        dataEntries.put(descriptor.getName(), new ClassLoaderEntry(bytes, resourceStream.getBaseUrl()));
    }

    private void loadJarStream(ResourceStream resourceStream) {
        BufferedInputStream bis = null;
        JarInputStream jis = null;

        try {
            bis = new BufferedInputStream(resourceStream.getInputStream());
            jis = new JarInputStream(bis);

            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory())
                    continue;

                byte[] b = new byte[1024];
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                int len;
                byte[] bytes;

                try {
                    while ((len = jis.read(b)) > 0)
                        out.write(b, 0, len);
                    bytes = out.toByteArray();
                } finally {
                    out.close();
                }

                jarEntries.put(jarEntry.getName(), new ClassLoaderEntry(bytes, resourceStream.getBaseUrl()));
            }
        } catch (IOException e) {
            throw new YarnClassLoaderException(e);
        } finally {
            YarnUtil.close(bis, jis);
        }
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) {
        Class result;
        byte[] classBytes;

        result = classes.get(className);

        if (result != null)
            return result;

        classBytes = classBytes(className);

        if (classBytes == null)
            return null;

        result = defineClass(className, classBytes, 0, classBytes.length);

        if (result == null)
            return null;

        if (result.getPackage() == null) {
            int lastDotIndex = className.lastIndexOf('.');
            String packageName = (lastDotIndex >= 0) ? className.substring(0, lastDotIndex) : "";
            definePackage(packageName, null, null, null, null, null, null, null);
        }

        if (resolveIt)
            resolveClass(result);

        classes.put(className, result);
        return result;
    }

    @Override
    public InputStream loadResource(String name) {
        byte[] arr = classBytes(name);

        if (arr == null) {
            ClassLoaderEntry classLoaderEntry = dataEntries.get(name);
            if (classLoaderEntry != null)
                arr = classLoaderEntry.getResourceBytes();
        }

        if (arr != null) {
            return new ByteArrayInputStream(arr);
        }

        return null;
    }

    @Override
    public URL findResource(String name) {
        URL url = getResourceURL(name);

        if (url != null)
            return url;

        return null;
    }

    private ClassLoaderEntry coalesce(String name, Map<String, ClassLoaderEntry>... resources) {
        for (Map<String, ClassLoaderEntry> map : resources) {
            ClassLoaderEntry entry = map.get(name);
            if (entry != null)
                return entry;
        }

        return null;
    }

    private byte[] classBytes(String name) {
        ClassLoaderEntry entry = coalesce(name, classEntries, jarEntries);

        if (entry != null)
            return entry.getResourceBytes();

        return null;
    }

    private URL getResourceURL(String name) {
        ClassLoaderEntry entry = coalesce(name, classEntries, dataEntries, jarEntries);

        if (entry != null) {
            if (entry.getBaseUrl() == null) {
                throw new YarnClassLoaderException("non-URL accessible resource");
            }

            try {
                return new URL(entry.getBaseUrl() + name);
            } catch (MalformedURLException e) {
                throw new YarnClassLoaderException(e);
            }
        }

        return null;
    }
}
