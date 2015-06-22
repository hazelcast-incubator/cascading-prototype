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

package com.hazelcast.yarn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;


public final class YarnUtil {
    public static final String JAR_NAME = "hazelcast-yarn.jar";

    public static final String LOGS =
            " 1>" + LOG_DIR_EXPANSION_VAR + "/stdout" +
                    " 2>" + LOG_DIR_EXPANSION_VAR + "/stderr";

    public static LocalResource createFileResource(Path file, FileSystem fs, LocalResourceType type)
            throws Exception {
        LocalResource resource = Records.newRecord(LocalResource.class);

        file = fs.makeQualified(file);
        FileStatus stat = fs.getFileStatus(file);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(file));
        resource.setSize(stat.getLen());
        resource.setTimestamp(stat.getModificationTime());
        resource.setType(type);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        return resource;
    }

    public static Path createHDFSFile(FileSystem fs, String src, String dst) throws Exception {
        Path dstPath = new Path(dst);
        fs.copyFromLocalFile(false, true, new Path(src), dstPath);
        return dstPath;
    }
}
