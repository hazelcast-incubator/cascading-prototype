package com.hazelcast.yarn.impl.application;

import java.net.URL;
import java.io.IOException;
import java.io.InputStream;
import java.io.DataInputStream;
import java.security.CodeSource;
import java.util.jar.JarInputStream;
import java.security.ProtectionDomain;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.yarn.impl.application.LocalizationResourceType.JAR;
import static com.hazelcast.yarn.impl.application.LocalizationResourceType.DATA;
import static com.hazelcast.yarn.impl.application.LocalizationResourceType.CLASS;

public class LocalizationResource {
    private final transient InputStream inputStream;
    private final LocalizationResourceDescriptor descriptor;

    private LocalizationResourceType getContentType(URL url) throws IOException {
        DataInputStream in = new DataInputStream(url.openStream());

        try {
            int magic = in.readInt();

            if (magic == 0xcafebabe) { // Check that is it class
                return CLASS;
            }
        } finally {
            in.close();
        }

        JarInputStream jarInputStream = new JarInputStream(url.openStream());

        try {
            if (jarInputStream.getNextJarEntry() != null) {
                return JAR;
            }
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return DATA;
        } finally {
            jarInputStream.close();
        }

        return DATA;
    }

    public LocalizationResource(Class clazz) throws IOException {
        ProtectionDomain protectionDomain = clazz.getProtectionDomain();
        String classAsPath = clazz.getName().replace('.', '/') + ".class";

        URL location;
        CodeSource codeSource;
        LocalizationResourceType localizationResourceType;

        if ((protectionDomain == null) ||
                ((codeSource = protectionDomain.getCodeSource()) == null) ||
                ((location = codeSource.getLocation()) == null) ||
                ((localizationResourceType = getContentType(location)) == DATA)) {
            this.inputStream = clazz.getClassLoader().getResourceAsStream(classAsPath);
            this.descriptor = new LocalizationResourceDescriptor(clazz.getName(), CLASS);
        } else {
            if (localizationResourceType != JAR) {
                throw new IllegalStateException("Something wrong with class=" + clazz + "it should be a part of jar archive");
            }

            this.inputStream = location.openStream();
            this.descriptor = new LocalizationResourceDescriptor(location.toString(), JAR);
        }

        checkNotNull(this.descriptor, "Descriptor is null");
        checkNotNull(this.inputStream, "InputStream is null");
    }

    public LocalizationResource(URL url) throws IOException {
        checkNotNull(url, "Url is null");
        this.inputStream = url.openStream();

        checkNotNull(this.inputStream, "InputStream is null");
        this.descriptor = new LocalizationResourceDescriptor(url.toString(), getContentType(url));
    }

    public LocalizationResource(InputStream inputStream, String name, LocalizationResourceType resourceType) {
        checkNotNull(inputStream, "InputStream is null");

        this.inputStream = inputStream;
        this.descriptor = new LocalizationResourceDescriptor(name, resourceType);
    }

    public InputStream openStream() {
        return this.inputStream;
    }

    public LocalizationResourceDescriptor getDescriptor() {
        return descriptor;
    }
}
