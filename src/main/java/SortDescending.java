import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable.Comparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortDescending {

    public static class CancelMapper
            extends Mapper<LongWritable, Text, FloatWritable, Text> {

        private final Text out_value = new Text();
        private final FloatWritable out_key = new FloatWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            // mapreduce outputs are tab separated
            String[] record = value.toString().split("\t");

            // set the key to be the opposite of the value, and the value to be the key from the first MR output
            out_value.set(record[0]);
            out_key.set(-Float.parseFloat(record[1]));
            context.write(out_key, out_value);

        }
    }

    public static class SortReducer
            extends Reducer<FloatWritable, Text, Text, FloatWritable> {

        private final Text original_key = new Text();

        public void reduce(FloatWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // Flip the key and value back
            for (Text val : values) {
                original_key.set(val);
                key.set(-key.get());
                context.write(original_key, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort descending");
        job.setJarByClass(SortDescending.class);
        job.setMapperClass(CancelMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Comparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
