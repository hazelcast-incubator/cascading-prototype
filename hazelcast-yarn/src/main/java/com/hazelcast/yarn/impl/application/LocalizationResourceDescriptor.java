package com.hazelcast.yarn.impl.application;

import java.io.Serializable;

public class LocalizationResourceDescriptor implements Serializable {
    private final String name;
    private final LocalizationResourceType resourceType;

    public LocalizationResourceDescriptor(String name, LocalizationResourceType resourceType) {
        this.name = name;
        this.resourceType = resourceType;
    }

    public LocalizationResourceType getResourceType() {
        return resourceType;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalizationResourceDescriptor that = (LocalizationResourceDescriptor) o;

        if (!name.equals(that.name)) return false;
        return resourceType == that.resourceType;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + resourceType.hashCode();
        return result;
    }
}
