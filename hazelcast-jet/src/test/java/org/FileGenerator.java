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

package org;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileGenerator {
    public static void main(String[] args) throws IOException {
        String dir = args[0];
        int files_count = Integer.valueOf(args[1]);
        int records_count = Integer.valueOf(args[2]);

        for (int idx = 1; idx <= files_count; idx++) {
            FileWriter fw = new FileWriter(new File(dir + "file_" + idx));
            StringBuilder sb = new StringBuilder();

            for (int i = 1; i <= records_count; i++) {
                sb.append(String.valueOf(i)).append(" \n");

                if (i % 4096 == 0) {
                    fw.write(sb.toString());
                    fw.flush();
                    sb = new StringBuilder();
                }
            }

            if (sb.length() > 0) {
                fw.write(sb.toString());
                fw.flush();
                fw.close();
                idx++;
            }
        }
    }
}
