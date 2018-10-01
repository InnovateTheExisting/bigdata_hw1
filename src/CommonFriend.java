import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonFriend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map() {

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    }

    public static void main(String[] args) throws Exception {

    }
}
