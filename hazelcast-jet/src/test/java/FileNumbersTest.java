import java.io.File;
import java.io.RandomAccessFile;

public class FileNumbersTest {
    public static void main(String[] args) {
        String file = "/hazelcast_work/partitions/file_1";
        File f = new File(file);
        long t = System.currentTimeMillis();

        try {
            for (int i = 0; i < 1000; i++) {
                RandomAccessFile raf = new RandomAccessFile(f, "r");
                System.out.println(i + " -> " + raf.length());
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
        }

        System.out.println("Delta=" + (System.currentTimeMillis() - t));
    }
}
