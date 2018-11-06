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

public class Top10 {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable mapOutKey = new LongWritable();
        private Text mapOutValue = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");
            mapOutValue.set(data[0]);
            String[] friends = data[1].split(",");
            mapOutKey.set((long) friends.length);
            context.write(mapOutKey, mapOutValue);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int count = 0;

        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (count < 100) {
                    // reduce output top 10
                    context.write(value, key);
                    count++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        String inputPath = remainingArgs[0];
        String outputPath = remainingArgs[1];

        // remove output directory if it exists
        Utils.removeOutputPath(config, outputPath);

        config = new Configuration();
        Job job = Job.getInstance(config, "Top10");

        job.setJarByClass(Top10.class);
        job.setMapperClass(Top10.Map.class);
        job.setReducerClass(Top10.Reduce.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            Utils.printFileContent(config, outputPath + "/part-r-00000");
            System.out.println("Success.");
        } else {
            System.out.println("Failed.");
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
