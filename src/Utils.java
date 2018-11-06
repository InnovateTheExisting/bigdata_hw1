import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.io.IOException;
import java.io.InputStream;

public class Utils {

    public static void removeOutputPath(Configuration config, String path) throws Exception {
        Path outputPath = new Path(path);
        FileSystem fs = outputPath.getFileSystem(config);
        if (fs.exists(outputPath)) {
            if (fs.delete(outputPath, true))
                System.out.println("Successfully delete " + outputPath + ".");
            else
                System.out.println("Failed to delete " + outputPath + ".");
        }
    }

    public static void printFileContent(Configuration config, String filePath) throws IOException {
        InputStream in = null;
        Path file = new Path(filePath);
        FileSystem fs = file.getFileSystem(config);
        try {
            in = fs.open(file);
            IOUtils.copyBytes(in, System.out, 4096, true);
        } finally {
            if (in != null) {
                IOUtils.closeStream(in);
            }
        }
    }

    public static int calculateAge(String val) {
        String[] births = val.split("/");

        LocalDate birthday = LocalDate.of(Integer.parseInt(births[2]), Integer.parseInt(births[0]), Integer.parseInt(births[1]));
        LocalDate current = LocalDate.now();
        long age = ChronoUnit.YEARS.between(birthday, current);

        return (int) age;
    }

}
