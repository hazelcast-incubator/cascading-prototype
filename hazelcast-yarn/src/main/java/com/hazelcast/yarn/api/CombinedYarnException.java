package com.hazelcast.yarn.api;

import java.util.List;
import java.io.PrintStream;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class CombinedYarnException extends RuntimeException {
    private final List<Throwable> errors;

    public CombinedYarnException(List<Throwable> errors) {
        checkNotNull(errors);
        this.errors = errors;
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public void printStackTrace(PrintStream s) {
        for (Throwable error : errors) {
            s.println("====== Exception ============");
            error.printStackTrace(s);
            s.println("====== Done      ==============");
        }
    }

}
