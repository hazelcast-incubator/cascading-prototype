package com.hazelcast.yarn.impl.application.localization;

import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.YarnException;
import com.hazelcast.yarn.api.application.localization.LocalizationStorage;
import com.hazelcast.yarn.api.application.localization.LocalizationType;

public class LocalizationStorageFactory {
    public static LocalizationStorage getLocalizationStorage(LocalizationType localizationType,
                                                             Config config,
                                                             String name)  {
        switch (localizationType) {
            case DISK:
                return new DiskLocalizationStorage(config, name);
            case MEMORY:
                return new InMemoryLocalizationStorage(config);
            default:
                throw new IllegalStateException("unknown localizationStorage type");
        }
    }
}
