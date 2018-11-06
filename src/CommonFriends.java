import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class CommonFriends {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text friendList = new Text();
        private Text mapOutKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");

            if (line.length == 2) {
                friendList.set(line[1]);
                String[] friends = line[1].split(",");
                long userID = Long.parseLong(line[0]);
                for (String friend : friends) {
                    long friendID = Long.parseLong(friend);

                    // make sure friends pair's first userid is less than second userid
                    if (userID < friendID) {
                        mapOutKey.set(line[0] + "," + friend);
                    } else if (userID > friendID) {
                        mapOutKey.set(friend + "," + line[0]);
                    }

                    context.write(mapOutKey, friendList);
                }
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private java.util.Map<String, String> keyMap = new HashMap<>();
        private Text mapOutKey = new Text();
        private Text result = new Text();

        public Map2() {
            keyMap.put("0", "1");
            keyMap.put("20", "28193");
            keyMap.put("1", "29826");
            keyMap.put("6222", "19272");
            keyMap.put("28041", "28056");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] keyVal = data[0].split(",");
            if (keyMap.containsKey(keyVal[0])) {
                if (keyMap.get(keyVal[0]).equals(keyVal[1])) {
                    mapOutKey.set(data[0]);
                    result.set(data[1]);
                    context.write(mapOutKey, result);
                    keyMap.remove(keyVal[0], keyVal[1]); // remove the key-value
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<String, String> entry : keyMap.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), new Text());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text commonFriend = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> friendSet = new HashSet<>();
            List<String> tempList = new ArrayList<>();

            for (Text text : values) {
                tempList.add(text.toString());
            }

            // final common friends list store in StringBuffer
            StringBuilder sb = new StringBuilder();
            if (tempList.size() == 2) {
                // store friend list 1 into hashset
                String friList1 = tempList.get(0);
                String[] friList1Arr = friList1.split(",");
                for (String friend : friList1Arr) {
                    friendSet.add(friend);
                }

                String friList2 = tempList.get(1);
                String[] friList2Arr = friList2.split(",");
                for (String friend : friList2Arr) {
                    // find common friends
                    if (friendSet.contains(friend)) {
                        sb.append(friend);
                        sb.append(",");
                        friendSet.remove(friend); // in case there are any duplicate friend
                    }
                }

                if (sb.length() > 0) {
                    // delete the last comma
                    sb.deleteCharAt(sb.length() - 1);
                    commonFriend.set(sb.toString());
                    context.write(key, commonFriend);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        String inputPath = remainingArgs[0];
        String outputPath = remainingArgs[1];
        String outputPath2 = remainingArgs[2];

        // remove output directory if exist
        Utils.removeOutputPath(config, outputPath);
        Utils.removeOutputPath(config, outputPath2);

        config = new Configuration();
        Job job1 = Job.getInstance(config, "CommonFriends");

        job1.setJarByClass(CommonFriends.class);
        job1.setMapperClass(CommonFriends.Map.class);
        job1.setReducerClass(CommonFriends.Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        if (job1.waitForCompletion(true)) {
//            Utils.printFileContent(config, outputPath + "/part-r-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }

        if (!job1.waitForCompletion(true))
            System.exit(1);


        // second job
        config = new Configuration();
        Job job2 = new Job(config, "ExtractPairs");

        job2.setJarByClass(CommonFriends.class);
        job2.setMapperClass(CommonFriends.Map2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // don't have reduce task
        job2.setNumReduceTasks(0);

        // use the temp output path as the input path of job2
        FileInputFormat.addInputPath(job2, new Path(outputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));

        if (job2.waitForCompletion(true)) {
            Utils.printFileContent(config, outputPath2 + "/part-m-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
