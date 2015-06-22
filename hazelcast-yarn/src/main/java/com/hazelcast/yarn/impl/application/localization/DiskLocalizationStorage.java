package com.hazelcast.yarn.impl.application.localization;

import java.io.*;

import com.hazelcast.config.Config;
import com.hazelcast.yarn.api.YarnException;
import com.hazelcast.yarn.impl.Chunk;
import com.hazelcast.yarn.impl.application.localization.classloader.ResourceStream;

public class DiskLocalizationStorage extends AbstractLocalizationStorage<File> {
    private final File tmpApplicationDir;

    private long fileNameCounter = 1;

    public DiskLocalizationStorage(Config config, String applicationName) {
        super(config);
        String containerDir = config.getYarnApplicationConfig(applicationName).getDefaultLocalizationDirectory();

        File dir;
        String postFix = "";
        int cnt = 1;

        do {
            dir = new File(containerDir + File.pathSeparator + "app_" + postFix + applicationName);
            postFix = String.valueOf(cnt);
            cnt++;

            int max = getConfig().getYarnApplicationConfig(applicationName).getDefaultApplicationDirectoryCreationAttemptsCount();

            if (cnt > max)
                throw new YarnException("Default application directory creation attempts count exceeded containerDir=" +
                        containerDir +
                        " defaultApplicationDirectoryCreationAttemptsCount=" + max);
        } while (!dir.mkdir());

        tmpApplicationDir = dir;
    }

    @Override
    public ResourceStream asResourceStream(File resource) throws IOException {
        return new ResourceStream(new FileInputStream(resource), resource.getAbsolutePath());
    }

    @Override
    protected File getResource(File resource, Chunk chunk) {
        try {
            File file = resource;

            if (file == null) {
                file = new File(tmpApplicationDir + File.pathSeparator + "resource" + fileNameCounter);
                fileNameCounter++;

                if (!file.exists())
                    if (!file.createNewFile())
                        throw new YarnException("Unable to create a file - localization fails");
            }

            if (!file.canWrite())
                throw new YarnException("Unable to write to еру file " + file.toURI().toURL() + " - file is not permitted to write");

            FileOutputStream fileOutputStream = new FileOutputStream(file, true);

            try {
                fileOutputStream.write(chunk.getBytes());
            } finally {
                fileOutputStream.close();
            }

            return file;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}