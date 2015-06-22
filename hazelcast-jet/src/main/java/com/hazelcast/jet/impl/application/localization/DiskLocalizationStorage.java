/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.application.localization;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.hazelcast.jet.impl.Chunk;
import com.hazelcast.jet.api.JetException;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.impl.application.localization.classloader.ResourceStream;


public class DiskLocalizationStorage extends AbstractLocalizationStorage<File> {
    private final File tmpApplicationDir;

    private long fileNameCounter = 1;

    public DiskLocalizationStorage(ApplicationContext applicationContext,
                                   String applicationName
    ) {
        super(applicationContext.getJetApplicationConfig());

        String containerDir = this.jetConfig.getLocalizationDirectory();

        File dir;
        String postFix = "";
        int cnt = 1;

        do {
            dir = new File(containerDir + File.pathSeparator + "app_" + postFix + applicationName);
            postFix = String.valueOf(cnt);
            cnt++;

            int max = this.jetConfig.getDefaultApplicationDirectoryCreationAttemptsCount();

            if (cnt > max) {
                throw new JetException(
                        "Default application directory creation attempts count exceeded containerDir="
                                + containerDir
                                + " defaultApplicationDirectoryCreationAttemptsCount="
                                + max
                );
            }
        } while (!dir.mkdir());

        this.tmpApplicationDir = dir;
    }

    @Override
    public ResourceStream asResourceStream(File resource) throws IOException {
        return new ResourceStream(new FileInputStream(resource), resource.toURI().toURL().toString());
    }

    @Override
    protected File getResource(File resource, Chunk chunk) {
        try {
            File file = resource;

            if (file == null) {
                file = new File(
                        this.tmpApplicationDir
                                + File.pathSeparator
                                + "resource"
                                + this.fileNameCounter
                );

                this.fileNameCounter++;

                if (!file.exists()) {
                    if (!file.createNewFile()) {
                        throw new JetException("Unable to create a file - localization fails");
                    }
                }
            }

            if (!file.canWrite()) {
                throw new JetException(
                        "Unable to write to the file "
                                + file.toURI().toURL()
                                + " - file is not permitted to write"
                );
            }

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
