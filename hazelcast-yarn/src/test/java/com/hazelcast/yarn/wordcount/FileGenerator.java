package com.hazelcast.yarn.wordcount;

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
