import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MinAge {
    public static class KeyPair implements WritableComparable<KeyPair> {
        private Text userId = new Text();
        private Text data = new Text();

        public KeyPair() {
        }

        public KeyPair(String userIdStr, String dataStr) {
            this.userId = new Text(userIdStr);
            this.data = new Text(dataStr);
        }

        public void setKey(String userIdStr) {
            this.userId.set(userIdStr);
        }

        public void setData(String dataStr) {
            this.data.set(dataStr);
        }

        public Text getKey() {
            return userId;
        }

        public Text getData() {
            return data;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            userId.write(dataOutput);
            data.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            userId.readFields(dataInput);
            data.readFields(dataInput);
        }

        @Override
        public int compareTo(KeyPair o) {
            int comp = userId.compareTo(o.getKey());
            if (comp != 0)
                return comp;
            return data.compareTo(o.getData());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof KeyPair) {
                KeyPair kp = (KeyPair) obj;
                return userId.equals(kp.getKey()) && data.equals(kp.getData());
            }
            return false;
        }

        @Override
        public int hashCode() {
            //  partition by userId
            return userId.hashCode();
        }
    }

    public static class AgeMap extends Mapper<LongWritable, Text, KeyPair, NullWritable> {
        private KeyPair mapOutKey = new KeyPair();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            int age = Utils.calculateAge(data[9]);
            // set composite key
            mapOutKey.setKey(data[0]);
            mapOutKey.setData("A:" + age);

            context.write(mapOutKey, NullWritable.get());
        }
    }

    public static class FriendListMap extends Mapper<LongWritable, Text, KeyPair, NullWritable> {
        private KeyPair mapOutKey = new KeyPair();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            mapOutKey.setData("B:" + data[0]);

            if (data.length != 2)
                return;

            String[] friends = data[1].split(",");
            for (String fri : friends) {
                mapOutKey.setKey(fri);
                context.write(mapOutKey, NullWritable.get());
            }
        }
    }

    public static class MinAgeReduce extends Reducer<KeyPair, NullWritable, Text, IntWritable> {
        private Map<String, Integer> minAgeMap = new HashMap<>();
        private int age;

        protected void reduce(KeyPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if (key.getData().toString().startsWith("A")) {
                String[] secondKey = key.getData().toString().split(":");
                age = Integer.parseInt(secondKey[1]);
            } else {
                String[] strs = key.getData().toString().split(":");
                if (minAgeMap.containsKey(strs[1])) {
                    int minAge = minAgeMap.get(strs[1]);
                    if (age < minAge)
                        minAgeMap.put(strs[1], age);
                } else {
                    minAgeMap.put(strs[1], age);
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : minAgeMap.entrySet()) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    // 2
    public static class SwapUserAgeMap extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutKey = new LongWritable();
        private Text mapOutValue = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            mapOutKey.set(Long.parseLong(data[1]));
            mapOutValue.set(data[0]);
            context.write(mapOutKey, mapOutValue);
        }

    }

    public static class MinAgeDesReduce extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int count = 0;

        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                if (count < 10) {
                    context.write(text, key);
                    count++;
                }
            }
        }
    }

    // 3
    public static class Top10UserMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text mapOutValue = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            mapOutKey.set(data[0]);
            mapOutValue.set(data[1]);
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class AddressMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapOutKey = new Text();
        private Text mapOutValue = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            mapOutKey.set(data[0]);
            mapOutValue.set(data[1] + "," + data[3] + "," + data[4] + "," + data[5] + "address");
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class FinalOutputReduce extends Reducer<Text, Text, Text, NullWritable> {
        private Text reduceOutKey = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String name = "";
            String addr = "";
            String age = "";
            for (Text text : values) {
                if (text.toString().endsWith("address")) {
                    String val = text.toString();
                    int len = val.length();
                    String addrName = val.substring(0, len - 7);
                    String[] strs = addrName.split(",", 2); // split name and address
                    name = strs[0];
                    addr = strs[1]; // delete address in the end
                } else {
                    age = text.toString();
                }
            }

            if (!name.isEmpty() && !addr.isEmpty() && !age.isEmpty()) {
                sb.append(name);
                sb.append(",");
                sb.append(addr);
                sb.append(",");
                sb.append(age);
                reduceOutKey.set(sb.toString());
                context.write(reduceOutKey, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        String socFilePath = remainingArgs[0];
        String userFilePath = remainingArgs[1];
        String outputPath = remainingArgs[2];
        String minAgeOutputPath = remainingArgs[3];
        String top10MinAgeOutputPath = remainingArgs[4];

        Utils.removeOutputPath(config, outputPath);
        Utils.removeOutputPath(config, minAgeOutputPath);
        Utils.removeOutputPath(config, top10MinAgeOutputPath);

        // job1 calculate minimum age of direct friend
        {
            config = new Configuration();
            Job job1 = Job.getInstance(config, "MinAge");

            job1.setJarByClass(MinAge.class);
            job1.setMapperClass(AgeMap.class);
            job1.setMapperClass(FriendListMap.class);
            job1.setReducerClass(MinAgeReduce.class);

            job1.setMapOutputKeyClass(KeyPair.class);
            job1.setMapOutputValueClass(NullWritable.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            MultipleInputs.addInputPath(job1, new Path(userFilePath), TextInputFormat.class, AgeMap.class);
            MultipleInputs.addInputPath(job1, new Path(socFilePath), TextInputFormat.class, FriendListMap.class);
            FileOutputFormat.setOutputPath(job1, new Path(minAgeOutputPath));

            if (job1.waitForCompletion(true)) {
                System.out.println("success.");
            } else {
                System.out.println("failed.");
            }

            if (!job1.waitForCompletion(true))
                System.exit(1);

            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }

        // job2 find top 10 minimum age
        {
            config = new Configuration();
            Job job2 = Job.getInstance(config, "TOP10MinAge");

            job2.setJarByClass(MinAge.class);
            job2.setMapperClass(SwapUserAgeMap.class);
            job2.setReducerClass(MinAgeDesReduce.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            // sort map key in descending order
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            FileInputFormat.addInputPath(job2, new Path(minAgeOutputPath));
            FileOutputFormat.setOutputPath(job2, new Path(top10MinAgeOutputPath));

            if (job2.waitForCompletion(true)) {
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            if (!job2.waitForCompletion(true))
                System.exit(1);
        }

        // job3 find the final result, including name, address, minimum age of friend
        {
            config = new Configuration();
            Job job3 = Job.getInstance(config, "TopUserAddressMinAge");

            job3.setJarByClass(MinAge.class);
            job3.setMapperClass(Top10UserMap.class);
            job3.setMapperClass(AddressMap.class);
            job3.setReducerClass(FinalOutputReduce.class);

            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(NullWritable.class);

            MultipleInputs.addInputPath(job3, new Path(top10MinAgeOutputPath), TextInputFormat.class, Top10UserMap.class);
            MultipleInputs.addInputPath(job3, new Path(userFilePath), TextInputFormat.class, AddressMap.class);
            FileOutputFormat.setOutputPath(job3, new Path(outputPath));

            if (job3.waitForCompletion(true)) {
                Utils.printFileContent(config, outputPath + "/part-r-00000");
                System.out.println("success");
            } else {
                System.out.println("fail");
            }

            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
    }
}
