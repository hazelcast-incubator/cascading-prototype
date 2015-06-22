package com.hazelcast.yarn.impl.application.localization.classloader;

import java.net.URL;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import com.hazelcast.yarn.api.application.localization.LocalizationStorage;

public class ApplicationClassLoader extends ClassLoader {
    protected final List<ProxyClassLoader> loaders = new ArrayList<ProxyClassLoader>();

    private final ProxyClassLoader systemLoader = new SystemLoader();
    private final ProxyClassLoader parentLoader = new ParentLoader();
    private final ProxyClassLoader currentLoader = new CurrentLoader();
    private final ProxyClassLoader threadLoader = new ThreadContextLoader();

    public ApplicationClassLoader(LocalizationStorage localizationStorage) {
        addDefaultLoaders();

        try {
            addLoader(new LocalizationClassLoader(localizationStorage.getResources()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addDefaultLoaders() {
        addLoader(systemLoader);
        addLoader(parentLoader);
        addLoader(currentLoader);

        if (Thread.currentThread().getContextClassLoader() != null)
            addLoader(threadLoader);
    }

    public void addLoader(ProxyClassLoader loader) {
        loaders.add(loader);
    }

    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        return (loadClass(className, true));
    }

    @Override
    public Class loadClass(String className, boolean resolveIt) throws ClassNotFoundException {
        if (className == null || className.trim().equals(""))
            return null;

        Class clazz = null;

        for (ProxyClassLoader l : loaders) {
            clazz = l.loadClass(className, resolveIt);
            if (clazz != null)
                break;
        }

        if (clazz == null)
            throw new ClassNotFoundException(className);

        return clazz;
    }

    @Override
    public URL getResource(String name) {
        if (name == null || name.trim().equals(""))
            return null;

        URL url = null;

        for (ProxyClassLoader l : loaders) {
            url = l.findResource(name);
            if (url != null)
                break;
        }

        return url;
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (name == null || name.trim().equals(""))
            return null;

        InputStream is = null;

        for (ProxyClassLoader l : loaders) {
            is = l.loadResource(name);
            if (is != null)
                break;
        }

        return is;

    }

    /**
     * System class loader
     */
    class SystemLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = findSystemClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getSystemResourceAsStream(name);

            if (is != null) {
                return is;
            }

            return null;
        }

        @Override
        public URL findResource(String name) {
            URL url = getSystemResource(name);

            if (url != null) {
                return url;
            }

            return null;
        }
    }

    /**
     * Parent class loader
     */
    class ParentLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = getParent().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getParent().getResourceAsStream(name);

            if (is != null) {
                return is;
            }
            return null;
        }


        @Override
        public URL findResource(String name) {
            URL url = getParent().getResource(name);

            if (url != null) {
                return url;
            }
            return null;
        }
    }

    /**
     * Current class loader
     */
    class CurrentLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = getClass().getClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = getClass().getClassLoader().getResourceAsStream(name);

            if (is != null) {
                return is;
            }

            return null;
        }


        @Override
        public URL findResource(String name) {
            URL url = getClass().getClassLoader().getResource(name);

            if (url != null) {
                return url;
            }

            return null;
        }
    }

    /**
     * Current class loader
     */
    class ThreadContextLoader implements ProxyClassLoader {
        @Override
        public Class loadClass(String className, boolean resolveIt) {
            Class result;

            try {
                result = Thread.currentThread().getContextClassLoader().loadClass(className);
            } catch (ClassNotFoundException e) {
                return null;
            }

            return result;
        }

        @Override
        public InputStream loadResource(String name) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);

            if (is != null) {
                return is;
            }

            return null;
        }

        @Override
        public URL findResource(String name) {
            URL url = Thread.currentThread().getContextClassLoader().getResource(name);

            if (url != null) {
                return url;
            }

            return null;
        }
    }
}
