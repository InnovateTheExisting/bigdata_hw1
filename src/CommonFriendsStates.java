import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class CommonFriendsStates {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private java.util.Map<String, String> userMap = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private String user1, user2;
        private boolean found = false;

        protected void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            // path where userdata.txt stored in hdfs
            String userDataPath = config.get("userdata");
            user1 = config.get("userA");
            user2 = config.get("userB");
            Path path = new Path(userDataPath);
            FileSystem fs = path.getFileSystem(config);
            if (!fs.exists(path)) {
                System.out.println("File \"" + userDataPath + "\" not exist.");
                return;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while (line != null) {
                String[] data = line.split(",");
                if (data.length == 10) {
                    // data[0]: userid, data[1]: user first name, data[5]: state
                    userMap.put(data[0], data[1] + ": " + data[5]);
                }
                line = br.readLine();
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] users = data[0].split(",");
            if (!users[0].equals(user1) || !users[1].equals(user2))
                return;

            found = true;
            outputKey.set(data[0]);
            String[] friends = data[1].split(",");
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (String fri : friends) {
                if (userMap.containsKey(fri)) {
                    sb.append(userMap.get(fri));
                    sb.append(", ");
                }
            }
            int len = sb.length();
            sb.delete(len - 2, len); // delete extra ", " at last
            sb.append("]");
            outputValue.set(sb.toString()); // [evangeline: Ohio, Charlotte: California]
            context.write(outputKey, outputValue);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!found) {
                outputKey.set("No such friends pair exists.");
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        String mutualFriendsPath = remainingArgs[0];
        String outputPath = remainingArgs[1];
        String userDataPath = remainingArgs[2];
        String targetUserA = remainingArgs[3];
        String targetUserB = remainingArgs[4];

        // remove output directory if exists
        Utils.removeOutputPath(config, outputPath);

        config = new Configuration();
        config.set("userdata", userDataPath); // set userdata file path to configuration object
        config.set("userA", targetUserA);
        config.set("userB", targetUserB);

        Job job = Job.getInstance(config, "CommonFriendsStates");
        job.setJarByClass(CommonFriendsStates.class);
        job.setMapperClass(CommonFriendsStates.Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        // the file of all mutual friend list (Q1)
        FileInputFormat.addInputPath(job, new Path(mutualFriendsPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            Utils.printFileContent(config, outputPath + "/part-m-00000");
            System.out.println("Success.");
        } else {
            System.out.println("Failed.");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
