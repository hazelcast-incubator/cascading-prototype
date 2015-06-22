package com.hazelcast.yarn.impl;

import sun.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.io.ByteArrayOutputStream;

public class YarnUtil {
    private static final Unsafe THE_UNSAFE;

    public static final long arrayBaseOffset;

    static {
        try {
            final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
                public Unsafe run() throws Exception {
                    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                    theUnsafe.setAccessible(true);
                    return (Unsafe) theUnsafe.get(null);
                }
            };

            THE_UNSAFE = AccessController.doPrivileged(action);
            arrayBaseOffset = (long) THE_UNSAFE.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }


    public static void close(Closeable... closeables) {
        if (closeables == null)
            return;

        Throwable last_error = null;

        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    last_error = e;
                }
            }
        }

        if (last_error != null)
            throw new RuntimeException(last_error);
    }

    public static byte[] readChunk(InputStream in, int chunkSize) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            byte[] b = new byte[chunkSize];
            out.write(b, 0, in.read(b));
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            close(out);
        }
    }

    public static byte[] readFully(InputStream in) {
        return readFully(in, false);
    }

    public static byte[] readFully(InputStream in, boolean closeInput) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            int len;
            byte[] b = new byte[1024];

            while ((len = in.read(b)) > 0)
                out.write(b, 0, len);

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (closeInput) {
                close(in, out);
            } else {
                close(out);
            }
        }
    }

    public static boolean isEmpty(Collection collection) {
        return (collection == null || collection.isEmpty());
    }

    public static boolean isEmpty(Object[] object) {
        return (object == null || object.length == 0);
    }


    /**
     * Get a handle on the Unsafe instance, used for accessing low-level concurrency
     * and memory constructs.
     *
     * @return The Unsafe
     */
    public static Unsafe getUnsafe() {
        return THE_UNSAFE;
    }
}
